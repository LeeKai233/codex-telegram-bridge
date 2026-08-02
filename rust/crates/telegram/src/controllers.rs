//! Role-local controller dispatch. Business effects remain in the engine; this
//! module guarantees a Telegram update cannot cross a bot-role boundary.

use crate::{RoutedUpdate, RuntimeBotRole, WorkflowAction};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControllerEffect {
    Noop,
    Accepted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerError(pub String);

pub trait WorkflowController {
    fn handle(&mut self, action: WorkflowAction) -> Result<ControllerEffect, ControllerError>;
}

pub struct FourRoleControllers<C, D, S> {
    pub control: C,
    pub discussion: D,
    pub status: S,
}

impl<C, D, S> FourRoleControllers<C, D, S>
where
    C: WorkflowController,
    D: WorkflowController,
    S: WorkflowController,
{
    pub fn dispatch(
        &mut self,
        role: RuntimeBotRole,
        routed: RoutedUpdate,
    ) -> Result<ControllerEffect, ControllerError> {
        match routed {
            RoutedUpdate::Ignore => Ok(ControllerEffect::Noop),
            RoutedUpdate::Dispatch(action) => match role {
                RuntimeBotRole::Control => self.control.handle(action),
                RuntimeBotRole::Discussion => self.discussion.handle(action),
                RuntimeBotRole::Status => self.status.handle(action),
                RuntimeBotRole::Alert => Ok(ControllerEffect::Noop),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TelegramActor, TelegramCallback, TelegramChatKind, WorkflowAction};

    #[derive(Default)]
    struct RecordingController(Vec<WorkflowAction>);

    impl WorkflowController for RecordingController {
        fn handle(&mut self, action: WorkflowAction) -> Result<ControllerEffect, ControllerError> {
            self.0.push(action);
            Ok(ControllerEffect::Accepted)
        }
    }

    #[test]
    fn dispatches_only_to_the_matching_role_controller() {
        let mut controllers = FourRoleControllers {
            control: RecordingController::default(),
            discussion: RecordingController::default(),
            status: RecordingController::default(),
        };
        let result = controllers
            .dispatch(
                RuntimeBotRole::Status,
                RoutedUpdate::Dispatch(WorkflowAction::Callback(TelegramCallback {
                    id: "callback".into(),
                    chat_id: -1004290500369,
                    chat_kind: TelegramChatKind::Supergroup,
                    message_id: 1,
                    data: "ds:status".into(),
                    actor: TelegramActor::default(),
                })),
            )
            .unwrap();
        assert_eq!(result, ControllerEffect::Accepted);
        assert!(controllers.control.0.is_empty());
        assert!(controllers.discussion.0.is_empty());
        assert_eq!(controllers.status.0.len(), 1);
    }
}
