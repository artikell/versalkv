use super::ServerContext;
use crate::engine::persistence::{AofState, PersistenceState};

impl ServerContext {
    pub(crate) fn prioritize_forkless_keys(
        &mut self,
        db_id: u32,
        keys: impl IntoIterator<Item = Vec<u8>>,
    ) -> bool {
        self.persistence.prioritize_forkless_keys(db_id, keys)
    }

    pub(crate) fn abort_upstream_transaction(&mut self, connection: usize) {
        if self
            .replication_mut()
            .end_upstream_transaction(connection)
            .is_some_and(|phase| phase.invalidates_baseline_on_abort())
        {
            self.aof_mut().invalidate_replication_baseline();
        }
    }

    pub(crate) fn abort_upstream_transactions(&mut self) -> Vec<usize> {
        let transactions = self.replication_mut().take_upstream_transactions();
        if transactions
            .values()
            .any(|phase| phase.invalidates_baseline_on_abort())
        {
            self.aof_mut().invalidate_replication_baseline();
        }
        transactions.into_keys().collect()
    }

    pub(crate) fn replication_aof_write_error(
        &self,
    ) -> Option<crate::foundation::io::protocol::Value> {
        if !self.config().replication.repl_after_aof_fsync {
            return None;
        }
        let pending_limit = self.config().replication.replica_output_buffer_hard_limit;
        if pending_limit != 0
            && self.replication().backlog.unreleased_bytes()
                >= pending_limit.max(self.config().server.repl_backlog_size.max(0) as usize)
        {
            return Some(crate::foundation::io::protocol::Value::error(
                "OOM local AOF replication backlog is waiting for synchronization",
            ));
        }
        if self.aof().last_write_status == crate::engine::persistence::OperationStatus::Err
            || self.aof().replication_baseline != crate::engine::persistence::AofBaseline::Ready
            || self
                .aof()
                .journal
                .as_ref()
                .is_none_or(|journal| !journal.appender.last_write_ok())
        {
            return Some(crate::foundation::io::protocol::Value::error(
                "MISCONF local AOF is not durable; data writes are disabled",
            ));
        }
        None
    }

    pub(crate) fn ensure_replication_snapshot_durable(&mut self) -> Result<(), String> {
        if self.config().replication.repl_after_aof_fsync {
            self.sync_replication_aof()?;
        }
        Ok(())
    }

    pub(crate) fn sync_replication_aof(&mut self) -> Result<(), String> {
        if self.aof().replication_baseline != crate::engine::persistence::AofBaseline::Ready {
            return Err("ERR local AOF baseline is not ready".to_string());
        }
        let journal = self
            .aof_mut()
            .journal
            .as_mut()
            .ok_or_else(|| "ERR local AOF baseline is not ready".to_string())?;
        journal
            .appender
            .flush(true)
            .map_err(|error| format!("MISCONF local AOF synchronization failed: {error}"))
    }

    // ── Persistence ──
    pub fn persistence(&self) -> &PersistenceState {
        &self.persistence
    }
    pub(crate) fn persistence_mut(&mut self) -> &mut PersistenceState {
        &mut self.persistence
    }
    pub(crate) fn drive_forkless_feed(&mut self) -> bool {
        let was_capturing = self.persistence.forkless_capture_in_progress();
        let mut additional_datasets = self
            .slot_imports
            .iter_mut()
            .map(|(id, dataset)| (id.as_str().as_bytes().to_vec(), dataset.dbs_mut()))
            .collect::<Vec<_>>();
        let released = self
            .persistence
            .drive_forkless_feed(&mut self.db.dbs, &mut additional_datasets);
        drop(additional_datasets);
        if was_capturing && !self.persistence.forkless_capture_in_progress() {
            self.set_forkless_capture_expiration_paused(false);
        }
        released
    }
    pub(crate) fn poll_forkless_save(
        &mut self,
        now_sec: i64,
    ) -> Option<crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome> {
        let mut additional_datasets = self
            .slot_imports
            .iter_mut()
            .map(|(id, dataset)| (id.as_str().as_bytes().to_vec(), dataset.dbs_mut()))
            .collect::<Vec<_>>();
        let outcome = self.persistence.poll_forkless_save(
            now_sec,
            &mut self.db.dbs,
            &mut additional_datasets,
        );
        drop(additional_datasets);
        if outcome.is_some() {
            self.set_forkless_capture_expiration_paused(false);
        }
        outcome
    }
    pub(crate) fn abort_forkless_save(&mut self, now_sec: i64) {
        let mut additional_datasets = self
            .slot_imports
            .iter_mut()
            .map(|(id, dataset)| (id.as_str().as_bytes().to_vec(), dataset.dbs_mut()))
            .collect::<Vec<_>>();
        self.persistence
            .abort_forkless_save(now_sec, &mut self.db.dbs, &mut additional_datasets);
        drop(additional_datasets);
        self.set_forkless_capture_expiration_paused(false);
    }
    pub(crate) fn set_forkless_capture_expiration_paused(&mut self, is_paused: bool) {
        // Fast path: only read the clock when a client pause is actually
        // scheduled. `ClientPauseState::is_paused` returns None immediately
        // when `end_time_ms == 0`, so the common no-pause hot path avoids a
        // `mstime()` syscall on every command.
        let client_pause_active = self.client_pause.end_time_ms != 0
            && self
                .client_pause
                .is_paused(crate::foundation::io::sys::time::mstime())
                != super::PauseAction::None;
        let should_skip_expire = is_paused
            || self.persistence.forkless_capture_in_progress()
            || client_pause_active
            || self.replication_aof_write_error().is_some()
            || (self.replication.is_replica() && self.rconfig.replication.replica_read_only);
        self.db.dbs.set_pause_skip_expire_all(should_skip_expire);
        for dataset in self.slot_imports.values_mut() {
            dataset
                .dbs_mut()
                .set_pause_skip_expire_all(should_skip_expire);
        }
    }
    pub fn aof(&self) -> &AofState {
        self.persistence.aof()
    }
    pub(crate) fn aof_mut(&mut self) -> &mut AofState {
        self.persistence.aof_mut()
    }
}
