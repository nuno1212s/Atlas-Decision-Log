pub mod config;
pub mod deciding_log;
pub mod decision_log;
pub mod decisions;
pub mod serialize;

use crate::config::DecLogConfig;
use crate::deciding_log::DecidingLog;
use crate::decision_log::DecisionLog;
use crate::decisions::CompletedDecision;
use crate::serialize::LogSerialization;
use atlas_common::error::*;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::ordering::{InvalidSeqNo, Orderable, SeqNo};
use atlas_common::serialization_helper::SerType;
use atlas_common::Err;
use atlas_core::executor::DecisionExecutorHandle;
use atlas_core::ordering_protocol::loggable::{
    LoggableOrderProtocol, PProof, PersistentOrderProtocolTypes,
};
use atlas_core::ordering_protocol::networking::serialize::OrderingProtocolMessage;
use atlas_core::ordering_protocol::{
    Decision, DecisionInfo, DecisionMetadata, OrderingProtocol, ProtocolConsensusDecision,
    ProtocolMessage,
};
use atlas_core::persistent_log::OperationMode;
use atlas_logging_core::decision_log::serialize::OrderProtocolLog;
use atlas_logging_core::decision_log::{
    DecLog, DecisionLogInitializer, DecisionLogPersistenceHelper, LoggedDecision, LoggingDecision,
    RangeOrderable,
};
use atlas_logging_core::persistent_log::PersistentDecisionLog;
use either::Either;
use log::{debug, error, info, trace};
use thiserror::Error;

/// Decision log implementation type
pub struct Log<RQ, OP, PL, EX>
where
    RQ: SerType,
    OP: LoggableOrderProtocol<RQ>,
{
    // The log of decisions that are currently ongoing
    deciding_log: DecidingLog<RQ, OP::Serialization, PL>,
    // The log of decisions that have already been decided since the last checkpoint
    decision_log: DecisionLog<RQ, OP::Serialization, OP::PersistableTypes>,
    // A reference to the persistent log
    persistent_log: PL,
    // An executor handle
    executor_handle: EX,
}

impl<RQ, OP, PL, EX> Orderable for Log<RQ, OP, PL, EX>
where
    RQ: SerType,
    OP: LoggableOrderProtocol<RQ>,
{
    fn sequence_number(&self) -> SeqNo {
        self.decision_log.last_execution().unwrap_or(SeqNo::ZERO)
    }
}

impl<RQ, OP, PL, EX> RangeOrderable for Log<RQ, OP, PL, EX>
where
    RQ: SerType,
    OP: LoggableOrderProtocol<RQ>,
{
    fn first_sequence(&self) -> SeqNo {
        self.decision_log.first_seq().unwrap_or(SeqNo::ZERO)
    }
}

type Ser<RQ, OP: LoggableOrderProtocol<RQ>> =
    LogSerialization<RQ, OP::Serialization, OP::PersistableTypes>;

impl<RQ, OP, PL, EX>
    DecisionLogPersistenceHelper<RQ, OP::Serialization, OP::PersistableTypes, Ser<RQ, OP>>
    for Log<RQ, OP, PL, EX>
where
    RQ: SerType,
    OP: LoggableOrderProtocol<RQ>,
    PL: Send,
    EX: Send,
{
    fn init_decision_log(
        _: (),
        proofs: Vec<PProof<RQ, OP::Serialization, OP::PersistableTypes>>,
    ) -> Result<DecLog<RQ, OP::Serialization, OP::PersistableTypes, Ser<RQ, OP>>> {
        Ok(DecisionLog::from_ordered_proofs(proofs))
    }

    fn decompose_decision_log(
        dec_log: DecisionLog<RQ, OP::Serialization, OP::PersistableTypes>,
    ) -> ((), Vec<PProof<RQ, OP::Serialization, OP::PersistableTypes>>) {
        ((), dec_log.into_proofs())
    }

    fn decompose_decision_log_ref(
        dec_log: &DecisionLog<RQ, OP::Serialization, OP::PersistableTypes>,
    ) -> (
        &(),
        Vec<&PProof<RQ, OP::Serialization, OP::PersistableTypes>>,
    ) {
        let mut proofs = Vec::with_capacity(dec_log.proofs().len());

        for proof in dec_log.proofs() {
            proofs.push(proof);
        }

        (&(), proofs)
    }
}

impl<RQ, OP, PL, EX> DecisionLogInitializer<RQ, OP, PL, EX> for Log<RQ, OP, PL, EX>
where
    RQ: SerType + 'static,
    OP: LoggableOrderProtocol<RQ>,
    PL: PersistentDecisionLog<
        RQ,
        OP::Serialization,
        OP::PersistableTypes,
        LogSerialization<RQ, OP::Serialization, OP::PersistableTypes>,
    >,
    EX: Send,
{
    fn initialize_decision_log(
        config: Self::Config,
        persistent_log: PL,
        executor_handle: EX,
    ) -> Result<Self>
    where
        PL: PersistentDecisionLog<
            RQ,
            OP::Serialization,
            OP::PersistableTypes,
            Self::LogSerialization,
        >,
        EX: DecisionExecutorHandle<RQ>,
        Self: Sized,
    {
        let dec_log =
            if let Some(dec_log) = persistent_log.read_decision_log(OperationMode::BlockingSync)? {
                dec_log
            } else {
                DecisionLog::new()
            };

        let deciding = if let Some(seq) = dec_log.last_execution() {
            DecidingLog::init(
                config.default_ongoing_capacity,
                seq.next(),
                persistent_log.clone(),
            )
        } else {
            DecidingLog::init(
                config.default_ongoing_capacity,
                SeqNo::ZERO,
                persistent_log.clone(),
            )
        };

        Ok(Log {
            deciding_log: deciding,
            decision_log: dec_log,
            persistent_log,
            executor_handle,
        })
    }
}

impl<RQ, OP, PL, EX> atlas_logging_core::decision_log::DecisionLog<RQ, OP> for Log<RQ, OP, PL, EX>
where
    RQ: SerType + 'static,
    OP: LoggableOrderProtocol<RQ>,
    PL: PersistentDecisionLog<
        RQ,
        OP::Serialization,
        OP::PersistableTypes,
        LogSerialization<RQ, OP::Serialization, OP::PersistableTypes>,
    >,
    EX: Send,
{
    type LogSerialization = LogSerialization<RQ, OP::Serialization, OP::PersistableTypes>;
    type Config = DecLogConfig;

    fn clear_sequence_number(&mut self, seq: SeqNo) -> Result<()>
where {
        let last_exec = self.decision_log.last_execution().unwrap_or(SeqNo::ZERO);

        match seq.index(last_exec) {
            Either::Left(_) | Either::Right(0) => {
                unreachable!("We are trying to clear a sequence number that has already been decided? How can that be cleared?")
            }
            Either::Right(_) => {
                self.deciding_log.clear_decision_at(seq);
            }
        }

        self.persistent_log
            .write_invalidate(OperationMode::NonBlockingSync(None), seq)?;

        Ok(())
    }

    fn clear_decisions_forward(&mut self, seq: SeqNo) -> Result<()> {
        self.deciding_log.clear_seq_forward_of(seq);

        Ok(())
    }

    fn decision_information_received(
        &mut self,
        decision_info: Decision<
            DecisionMetadata<RQ, OP::Serialization>,
            ProtocolMessage<RQ, OP::Serialization>,
            RQ,
        >,
    ) -> Result<MaybeVec<LoggedDecision<RQ>>> {
        let seq = decision_info.sequence_number();

        let index = seq.index(self.decision_log.last_execution().unwrap_or(SeqNo::ZERO));

        match index {
            Either::Left(_) => {
                error!("Received decision information about a decision that has already been made");
            }
            Either::Right(index) => {
                trace!("Received information about decision {:?}", decision_info);

                decision_info
                    .into_decision_info()
                    .into_iter()
                    .for_each(|info| match info {
                        DecisionInfo::DecisionDone(done) => {
                            self.deciding_log.complete_decision(seq, done);
                        }
                        DecisionInfo::PartialDecisionInformation(messages) => {
                            messages.into_iter().for_each(|message| {
                                self.deciding_log.decision_progressed(seq, message);
                            });
                        }
                        DecisionInfo::DecisionMetadata(metadata) => {
                            self.deciding_log.decision_metadata(seq, metadata);
                        }
                    });
            }
        }

        let decisions = self.deciding_log.complete_pending_decisions();

        if decisions.is_empty() {
            Ok(MaybeVec::None)
        } else {
            Ok(self.execute_decisions(decisions)?)
        }
    }

    fn install_proof(
        &mut self,
        proof: PProof<RQ, OP::Serialization, OP::PersistableTypes>,
    ) -> Result<MaybeVec<LoggedDecision<RQ>>> {
        if let Some(decision) = self.decision_log.last_decision_ref() {
            match proof.sequence_number().index(decision.sequence_number()) {
                Either::Left(_) | Either::Right(0) => {
                    return Err!(DecisionLogError::AlreadyExistsProof(
                        proof.sequence_number()
                    ));
                }
                Either::Right(1) => {
                    self.decision_log.append_proof(proof.clone())?;
                }
                Either::Right(_) => {
                    return Err!(DecisionLogError::AttemptedDecisionIsAhead(
                        proof.sequence_number()
                    ));
                }
            }
        } else {
            self.decision_log.append_proof(proof.clone())?;
        }

        let protocol_decision = OP::get_requests_in_proof(&proof)?;

        self.persistent_log
            .write_proof(OperationMode::NonBlockingSync(None), proof)?;

        self.deciding_log
            .advance_to_seq(self.decision_log.last_execution().unwrap().next());

        self.execute_decision_from_proofs(MaybeVec::One(protocol_decision))
    }

    fn install_log(
        &mut self,
        dec_log: DecLog<RQ, OP::Serialization, OP::PersistableTypes, Self::LogSerialization>,
    ) -> Result<MaybeVec<LoggedDecision<RQ>>> {
        info!(
            "Installing a decision log with bounds {:?} - {:?}. Current bounds are: {:?} - {:?}",
            dec_log.first_seq(),
            dec_log.last_execution(),
            self.decision_log.first_seq(),
            self.decision_log.last_execution()
        );

        self.decision_log = dec_log;

        let mut requests = Vec::new();

        // reset the stored log as we are going to receive
        self.persistent_log
            .reset_log(OperationMode::NonBlockingSync(None))?;

        for proof in self.decision_log.proofs() {
            let protocol_decision = OP::get_requests_in_proof(proof)?;

            requests.push(protocol_decision);

            self.persistent_log
                .write_proof(OperationMode::NonBlockingSync(None), proof.clone())?;
        }

        let last_decision = self.decision_log.last_execution();

        if let Some(seq) = last_decision {
            self.deciding_log.advance_to_seq(seq.next());
        } else {
            // We received an empty decision log? Weird

            self.deciding_log.reset_to_zero();
        }

        self.execute_decision_from_proofs(MaybeVec::from_many(requests))
    }

    fn snapshot_log(
        &mut self,
    ) -> Result<DecLog<RQ, OP::Serialization, OP::PersistableTypes, Self::LogSerialization>> {
        Ok(self.decision_log.clone())
    }

    fn current_log(
        &self,
    ) -> Result<&DecLog<RQ, OP::Serialization, OP::PersistableTypes, Self::LogSerialization>> {
        Ok(&self.decision_log)
    }

    fn state_checkpoint(&mut self, seq: SeqNo) -> Result<()> {
        let deleted_client_rqs = self.decision_log.clear_until_seq(seq);

        Ok(())
    }

    fn verify_sequence_number(
        &self,
        seq_no: SeqNo,
        proof: &PProof<RQ, OP::Serialization, OP::PersistableTypes>,
    ) -> Result<bool> {
        if seq_no != proof.sequence_number() {
            return Ok(false);
        }

        //TODO:

        Ok(true)
    }

    fn sequence_number_with_proof(
        &self,
    ) -> Result<Option<(SeqNo, PProof<RQ, OP::Serialization, OP::PersistableTypes>)>> {
        if let Some(decision) = self.decision_log.last_decision() {
            Ok(Some((decision.sequence_number(), decision)))
        } else {
            Ok(None)
        }
    }

    fn get_proof(
        &self,
        seq: SeqNo,
    ) -> Result<Option<PProof<RQ, OP::Serialization, OP::PersistableTypes>>> {
        return if let Some(decision) = self.decision_log.last_execution() {
            if seq > decision {
                Err!(DecisionLogError::NoProofBySeq(seq, decision))
            } else {
                Ok(self.decision_log.get_proof(seq))
            }
        } else {
            Ok(None)
        };
    }
}

impl<RQ, OP, PL, EX> Log<RQ, OP, PL, EX>
where
    RQ: SerType,
    OP: LoggableOrderProtocol<RQ>,
    PL: Send,
    EX: Send,
{
    fn execute_decision_from_proofs(
        &mut self,
        batches: MaybeVec<ProtocolConsensusDecision<RQ>>,
    ) -> Result<MaybeVec<LoggedDecision<RQ>>>
    where
        PL: PersistentDecisionLog<RQ, OP::Serialization, OP::PersistableTypes, Ser<RQ, OP>>,
    {
        let mut decisions_made = MaybeVec::builder();

        for protocol_decision in batches.into_iter() {
            let (seq, update, client_rqs, batch_digest) = protocol_decision.into();

            let logging_info = LoggingDecision::Proof(seq);

            if let Some(to_execute) = self
                .persistent_log
                .wait_for_full_persistence(update, logging_info)?
            {
                decisions_made.push(LoggedDecision::from_decision_with_execution(
                    seq, client_rqs, to_execute,
                ));
            } else {
                decisions_made.push(LoggedDecision::from_decision(seq, client_rqs));
            }
        }

        Ok(decisions_made.build())
    }

    fn execute_decisions(
        &mut self,
        decisions: Vec<CompletedDecision<RQ, OP::Serialization>>,
    ) -> Result<MaybeVec<LoggedDecision<RQ>>>
    where
        PL: PersistentDecisionLog<RQ, OP::Serialization, OP::PersistableTypes, Ser<RQ, OP>>,
    {
        debug!(
            "Sending {} decisions to be executed by the executor",
            decisions.len()
        );

        let mut decisions_made = MaybeVec::builder();

        for decision in decisions {
            let (seq, metadata, messages, protocol_decision, logged_info) = decision.into();

            let proof = OP::init_proof_from_scm(metadata, messages)?;

            self.decision_log.append_proof(proof)?;

            let (seq, batch, client_rqs, batch_digest) = protocol_decision.into();

            if let Some(batch) = self
                .persistent_log
                .wait_for_full_persistence(batch, logged_info)?
            {
                decisions_made.push(LoggedDecision::from_decision_with_execution(
                    seq, client_rqs, batch,
                ));
            } else {
                decisions_made.push(LoggedDecision::from_decision(seq, client_rqs));
            }
        }

        Ok(decisions_made.build())
    }
}

#[derive(Error, Debug)]
pub enum DecisionLogError {
    #[error("There is no proof by the seq {0:?}. The last execution was {1:?}")]
    NoProofBySeq(SeqNo, SeqNo),
    #[error("There already exists a decision at the sequence number {0:?}")]
    AlreadyExistsProof(SeqNo),
    #[error("Decision that was attempted to append is ahead of our stored decisions {0:?}")]
    AttemptedDecisionIsAhead(SeqNo),
}
