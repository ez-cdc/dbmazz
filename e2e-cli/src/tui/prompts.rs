//! Interactive prompt wrappers around `cliclack` with consistent EZ-CDC UX.
//!
//! All prompts handle Ctrl+C gracefully by returning `PromptError::Cancelled`.

use std::fmt::Display;

use thiserror::Error;

/// Errors that can occur during interactive prompts.
#[derive(Debug, Error)]
pub enum PromptError {
    /// User pressed Ctrl+C or ESC.
    #[error("cancelled by user")]
    Cancelled,

    /// Underlying I/O error from the prompt library.
    #[error("prompt I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Check if stdin is connected to an interactive terminal.
pub fn is_interactive() -> bool {
    atty::is(atty::Stream::Stdin)
}

/// Show a select menu and return the user's choice.
///
/// Each item is a tuple of `(value, label, hint)`. The label is displayed
/// and the hint appears as a description. Returns the selected `T` value.
pub fn select<T>(message: &str, items: Vec<(T, String, String)>) -> Result<T, PromptError>
where
    T: Clone + Eq + Display,
{
    if items.is_empty() {
        return Err(PromptError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no items to select from",
        )));
    }

    let mut prompt = cliclack::select(message);
    for (value, label, hint) in &items {
        prompt = prompt.item(value.clone(), label, hint);
    }

    prompt.interact().map_err(|e| {
        if is_ctrl_c_error(&e) {
            PromptError::Cancelled
        } else {
            PromptError::Io(e)
        }
    })
}

/// Show a yes/no confirmation prompt.
pub fn confirm(message: &str, default: bool) -> Result<bool, PromptError> {
    let result = cliclack::confirm(message)
        .initial_value(default)
        .interact()
        .map_err(|e| {
            if is_ctrl_c_error(&e) {
                PromptError::Cancelled
            } else {
                PromptError::Io(e)
            }
        })?;

    Ok(result)
}

/// Show a text input prompt with an optional default value.
pub fn text(message: &str, default: &str) -> Result<String, PromptError> {
    let mut prompt = cliclack::input(message);
    if !default.is_empty() {
        prompt = prompt.default_input(default);
    }

    prompt.interact().map_err(|e| {
        if is_ctrl_c_error(&e) {
            PromptError::Cancelled
        } else {
            PromptError::Io(e)
        }
    })
}

/// Show a password input prompt (characters hidden). Empty input is
/// allowed because several supported sinks (StarRocks with default
/// root, dev-local Postgres, etc.) have no password by design.
pub fn password(message: &str) -> Result<String, PromptError> {
    cliclack::password(message)
        .allow_empty()
        .interact()
        .map_err(|e| {
            if is_ctrl_c_error(&e) {
                PromptError::Cancelled
            } else {
                PromptError::Io(e)
            }
        })
}

/// Heuristic to detect Ctrl+C / interrupt errors from cliclack.
///
/// cliclack doesn't expose a typed error for interrupts, so we check the
/// error kind and message.
fn is_ctrl_c_error(err: &std::io::Error) -> bool {
    matches!(err.kind(), std::io::ErrorKind::Interrupted)
        || err.to_string().to_lowercase().contains("interrupt")
        || err.to_string().to_lowercase().contains("ctrl")
}
