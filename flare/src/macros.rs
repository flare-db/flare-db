#[macro_export]
macro_rules! check_argument {
    // check_argument!(condition, ErrorVariant)  ← pass a BeamTranslationError directly
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err);  // $err is already a BeamTranslationError, no .into() needed
        }
    };
    // check_argument!(condition, "format string {}", value)
    ($cond:expr, $fmt:literal $(, $arg:expr)*) => {
        if !$cond {
            return Err(crate::errors::BeamTranslationError::InvalidArgument(
                format!($fmt $(, $arg)*)
            ).into());
        }
    };
}
