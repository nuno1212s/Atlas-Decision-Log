pub mod decisions;
pub mod decision_log;
pub mod deciding_log;
pub mod config;

use atlas_common::ordering::{Orderable, SeqNo};
use atlas_core::ordering_protocol::networking::serialize::OrderingProtocolMessage;
use atlas_core::ordering_protocol::{DecisionMetadata, OrderingProtocol};
use atlas_core::ordering_protocol::loggable::{LoggableOrderProtocol, PersistentOrderProtocolTypes, PProof};
use atlas_core::persistent_log::{OperationMode, PersistentDecisionLog};
use atlas_core::smr::smr_decision_log::{DecLog, StoredConsensusMessage};
use atlas_smr_application::serialize::ApplicationData;
use crate::config::DecLogConfig;
use crate::deciding_log::DecidingLog;
use crate::decision_log::DecisionLog;

/// Decision log implementation type
pub struct Log<D, OP, NT, PL> where D: ApplicationData,
                                    OP: LoggableOrderProtocol<D, NT, PL>, {
    // The log of decisions that are currently ongoing
    deciding_log: DecidingLog<D, OP::Serialization, OP::PersistableTypes>,

    // The log of decisions that have already been decided since the last checkpoint
    decision_log: DecisionLog<D, OP::Serialization, OP::PersistableTypes>,

    persistent_log: PL,
}

impl<D, OP, NT, PL> Orderable for Log<D, OP, NT, PL> where D: ApplicationData, OP: LoggableOrderProtocol<D, NT, PL> {
    fn sequence_number(&self) -> SeqNo {
        self.decision_log.last_execution().unwrap_or(SeqNo::ZERO)
    }
}

impl<D, OP, NT, PL> atlas_core::smr::smr_decision_log::DecisionLog<D, OP, NT, PL> for Log<D, OP, NT, PL>
    where D: ApplicationData, OP: LoggableOrderProtocol<D, NT, PL> {
    type LogSerialization = ();
    type Config = DecLogConfig;

    fn initialize_decision_log(config: Self::Config, persistent_log: PL) -> atlas_common::error::Result<Self>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        let dec_log = if let Some(dec_log) = persistent_log.read_decision_log(OperationMode::BlockingSync)? {
            dec_log
        } else {
            DecisionLog::new()
        };

        let deciding = if let Some(seq) = dec_log.last_execution() {
            DecidingLog::init(config.default_ongoing_capacity, seq.next())
        } else {
            DecidingLog::init(config.default_ongoing_capacity, SeqNo::ZERO)
        };

        Ok(Log {
            deciding_log: deciding,
            decision_log: dec_log,
            persistent_log,
        })
    }

    fn clear_sequence_number(&mut self, seq: SeqNo) -> atlas_common::error::Result<()>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {


        todo!()
    }

    fn clear_decisions_forward(&mut self, seq: SeqNo) -> atlas_common::error::Result<()>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {

        self.deciding_log.clear_seq_forward_of(seq);

        Ok(())

    }

    fn sequence_number_advanced(&mut self, seq: SeqNo, message: StoredConsensusMessage<D, OP::Serialization, OP::PersistableTypes>) -> atlas_common::error::Result<()>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        todo!()
    }

    fn sequence_number_decided(&mut self, seq: SeqNo, metadata: DecisionMetadata<D, OP::Serialization>) -> atlas_common::error::Result<()>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        self.deciding_log.complete_decision(seq, metadata);

        for completed_decision in self.deciding_log.complete_pending_decisions() {
            let (metadata, messages) = completed_decision.into_components();

            let proof = OP::init_proof_from_scm(metadata, messages);

            self.decision_log.append_proof(proof)
        }

        Ok(())
    }

    fn sequence_number_decided_with_full_proof(&mut self, proof: PProof<D, OP::Serialization, OP::PersistableTypes>) -> atlas_common::error::Result<()>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        todo!()
    }

    fn install_log(&mut self, order_protocol: &OP, dec_log: DecLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization>) -> atlas_common::error::Result<Vec<D::Request>>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        todo!()
    }

    fn snapshot_log(&mut self) -> atlas_common::error::Result<DecLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization>>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        todo!()
    }

    fn current_log(&self) -> atlas_common::error::Result<&DecLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization>>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        todo!()
    }

    fn state_checkpoint(&self, seq: SeqNo) -> atlas_common::error::Result<()>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        todo!()
    }

    fn verify_sequence_number(&self, seq_no: SeqNo, proof: &PProof<D, OP::Serialization, OP::PersistableTypes>) -> atlas_common::error::Result<bool> {
        todo!()
    }

    fn sequence_number_with_proof(&self) -> atlas_common::error::Result<Option<(SeqNo, PProof<D, OP::Serialization, OP::PersistableTypes>)>> {
        todo!()
    }

    fn get_proof(&self, seq: SeqNo) -> atlas_common::error::Result<Option<PProof<D, OP::Serialization, OP::PersistableTypes>>>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        todo!()
    }
}