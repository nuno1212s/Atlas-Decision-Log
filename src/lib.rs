pub mod decisions;
pub mod decision_log;
pub mod deciding_log;
pub mod config;
pub mod serialize;

use either::Either;
use log::error;
use atlas_common::ordering::{InvalidSeqNo, Orderable, SeqNo};
use atlas_core::messages::StoredRequestMessage;
use atlas_core::ordering_protocol::networking::serialize::OrderingProtocolMessage;
use atlas_core::ordering_protocol::{Decision, DecisionInfo, DecisionMetadata, OrderingProtocol, ProtocolMessage};
use atlas_core::ordering_protocol::loggable::{LoggableOrderProtocol, PersistentOrderProtocolTypes, PProof};
use atlas_core::persistent_log::{OperationMode, PersistentDecisionLog};
use atlas_core::smr::smr_decision_log::{DecLog, StoredConsensusMessage, wrap_loggable_message};
use atlas_smr_application::serialize::ApplicationData;
use crate::config::DecLogConfig;
use crate::deciding_log::DecidingLog;
use crate::decision_log::DecisionLog;
use crate::serialize::LogSerialization;

/// Decision log implementation type
pub struct Log<D, OP, NT, PL> where D: ApplicationData,
                                    OP: LoggableOrderProtocol<D, NT>, {
    // The log of decisions that are currently ongoing
    deciding_log: DecidingLog<D, OP::Serialization, OP::PersistableTypes>,

    // The log of decisions that have already been decided since the last checkpoint
    decision_log: DecisionLog<D, OP::Serialization, OP::PersistableTypes>,

    persistent_log: PL,
}

impl<D, OP, NT, PL> Orderable for Log<D, OP, NT, PL> where D: ApplicationData, OP: LoggableOrderProtocol<D, NT> {
    fn sequence_number(&self) -> SeqNo {
        self.decision_log.last_execution().unwrap_or(SeqNo::ZERO)
    }
}

impl<D, OP, NT, PL> atlas_core::smr::smr_decision_log::DecisionLog<D, OP, NT, PL> for Log<D, OP, NT, PL>
    where D: ApplicationData, OP: LoggableOrderProtocol<D, NT> {
    type LogSerialization = LogSerialization<D, OP::Serialization, OP::PersistableTypes>;
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
        let last_exec = self.decision_log.last_execution().unwrap_or(SeqNo::ZERO);

        match seq.index(last_exec) {
            Either::Left(_) | Either::Right(0) => {
                unreachable!("")
            }
            Either::Right(_) => {
                self.deciding_log.clear_decision_at(seq);
            }
        }

        Ok(())
    }

    fn clear_decisions_forward(&mut self, seq: SeqNo) -> atlas_common::error::Result<()>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        self.deciding_log.clear_seq_forward_of(seq);

        Ok(())
    }

    fn decision_information_received(&mut self, decision_info: Decision<DecisionMetadata<D, OP::Serialization>, ProtocolMessage<D, OP::Serialization>, D::Request>) -> atlas_common::error::Result<()> where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        let seq = decision_info.sequence_number();

        let index = seq.index(self.decision_log.last_execution().unwrap_or(SeqNo::ZERO));

        match index {
            Either::Left(_) | Either::Right(0) => {
                error!("Received decision information about a decision that has already been made");
            }
            Either::Right(index) => {
                decision_info.into_decision_info().into_iter().for_each(|info| {
                    match info {
                        DecisionInfo::DecisionDone(done) => {
                            self.deciding_log.complete_decision(seq, done)?;
                        }
                        DecisionInfo::PartialDecisionInformation(messages) => {
                            messages.into_iter().for_each(|message| {
                                let message = OP::protocol_message_to_loggable_message(message);

                                let message = wrap_loggable_message(message);

                                self.deciding_log.decision_progressed(seq, message)?;
                            });
                        }
                        DecisionInfo::DecisionMetadata(metadata) => {
                            self.deciding_log.decision_metadata(seq, metadata)?;
                        }
                    }
                });
            }
        }

        Ok(())
    }

    fn install_log(&mut self, order_protocol: &OP,
                   dec_log: DecLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization>) -> atlas_common::error::Result<(SeqNo, Vec<StoredRequestMessage<D::Request>>)>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        self.decision_log = dec_log;

        let mut requests = Vec::new();

        self.decision_log.proofs().iter().for_each(|proof| {
            let mut p_requests = OP::get_requests_in_proof(proof);

            requests.append(&mut p_requests);
        });

        Ok((self.decision_log.last_execution().unwrap_or(SeqNo::ZERO), requests))
    }

    fn snapshot_log(&mut self) -> atlas_common::error::Result<DecLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization>>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        Ok(self.decision_log.clone())
    }

    fn current_log(&self) -> atlas_common::error::Result<&DecLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization>>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {
        Ok(&self.decision_log)
    }

    fn state_checkpoint(&mut self, seq: SeqNo) -> atlas_common::error::Result<()>
        where PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, Self::LogSerialization> {

        let deleted_client_rqs = self.decision_log.clear_until_seq(seq);

        Ok(())
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