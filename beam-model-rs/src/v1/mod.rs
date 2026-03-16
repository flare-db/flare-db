pub mod org {
    pub mod apache {
        pub mod beam {
            pub mod model {

                pub mod pipeline {
                    pub mod v1 {
                        include!("org.apache.beam.model.pipeline.v1.rs");
                    }
                }

                pub mod fn_execution {
                    pub mod v1 {
                        include!("org.apache.beam.model.fn_execution.v1.rs");
                    }
                }

                pub mod job_management {
                    pub mod v1 {
                        include!("org.apache.beam.model.job_management.v1.rs");
                    }
                }

                pub mod expansion {
                    pub mod v1 {
                        include!("org.apache.beam.model.expansion.v1.rs");
                    }
                }

                pub mod interactive {
                    pub mod v1 {
                        include!("org.apache.beam.model.interactive.v1.rs");
                    }
                }
            }
        }
    }
}

pub use org::apache::beam::model::{
    expansion::v1::*, fn_execution::v1::*, interactive::v1::*, job_management::v1::*,
    pipeline::v1::*,
};
