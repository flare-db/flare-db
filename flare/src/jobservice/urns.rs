pub mod beam_urns {
    use std::collections::HashMap;
    use std::sync::LazyLock;

    // Primitives
    pub const CREATE_TRANSFORM: &str = "beam:transform:create:v1";
    pub const PAR_DO_TRANSFORM: &str = "beam:transform:pardo:v1";
    pub const FLATTEN_TRANSFORM: &str = "beam:transform:flatten:v1";
    pub const GROUP_BY_KEY_TRANSFORM: &str = "beam:transform:group_by_key:v1";
    pub const GROUP_BY_KEY_WRAPPER_TRANSFORM: &str = "beam:transform:group_by_key_wrapper:v1";
    pub const IMPULSE_TRANSFORM: &str = "beam:transform:impulse:v1";
    pub const ASSIGN_WINDOWS_TRANSFORM: &str = "beam:transform:window_into:v1";
    pub const TEST_STREAM_TRANSFORM: &str = "beam:transform:teststream:v1";
    pub const MAP_WINDOWS_TRANSFORM: &str = "beam:transform:map_windows:v1";
    pub const MERGE_WINDOWS_TRANSFORM: &str = "beam:transform:merge_windows:v1";
    pub const TO_STRING_TRANSFORM: &str = "beam:transform:to_string:v1";
    pub const MANAGED_TRANSFORM: &str = "beam:transform:managed:v1";

    //Deprecated
    pub const CREATE_VIEW_TRANSFORM: &str = "beam:transform:create_view:v1";

    pub const PRIMITIVES: &[&str] = &[
        CREATE_TRANSFORM,
        PAR_DO_TRANSFORM,
        FLATTEN_TRANSFORM,
        GROUP_BY_KEY_TRANSFORM,
        GROUP_BY_KEY_WRAPPER_TRANSFORM,
        IMPULSE_TRANSFORM,
        ASSIGN_WINDOWS_TRANSFORM,
        TEST_STREAM_TRANSFORM,
        MAP_WINDOWS_TRANSFORM,
        MERGE_WINDOWS_TRANSFORM,
        TO_STRING_TRANSFORM,
        MANAGED_TRANSFORM,
    ];

    pub const FLARE: &[&str] = &[GROUP_BY_KEY_TRANSFORM, IMPULSE_TRANSFORM];

    // Composites
    pub const COMBINE_PER_KEY_TRANSFORM_URN: &str = "beam:transform:combine_per_key:v1";
    pub const COMBINE_GLOBALLY_TRANSFORM_URN: &str = "beam:transform:combine_globally:v1";
    pub const RESHUFFLE_URN: &str = "beam:transform:reshuffle:v1";
    pub const REDISTRIBUTE_BY_KEY_URN: &str = "beam:transform:redistribute_by_key:v1";
    pub const REDISTRIBUTE_ARBITRARILY_URN: &str = "beam:transform:redistribute_arbitrarily:v1";
    pub const WRITE_FILES_TRANSFORM_URN: &str = "beam:transform:write_files:v1";
    pub const GROUP_INTO_BATCHES_WITH_SHARDED_KEY_URN: &str =
        "beam:transform:group_into_batches_with_sharded_key:v1";
    pub const PUBSUB_READ: &str = "beam:transform:pubsub_read:v1";
    pub const PUBSUB_WRITE: &str = "beam:transform:pubsub_write:v1";
    pub const PUBSUB_WRITE_DYNAMIC: &str = "beam:transform:pubsub_write:v2";

    pub const COMPOSITES: &[&str] = &[
        COMBINE_PER_KEY_TRANSFORM_URN,
        COMBINE_GLOBALLY_TRANSFORM_URN,
        RESHUFFLE_URN,
        REDISTRIBUTE_BY_KEY_URN,
        REDISTRIBUTE_ARBITRARILY_URN,
        WRITE_FILES_TRANSFORM_URN,
        GROUP_INTO_BATCHES_WITH_SHARDED_KEY_URN,
        PUBSUB_READ,
        PUBSUB_WRITE,
        PUBSUB_WRITE_DYNAMIC,
    ];

    // CombineComponents
    pub const COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN: &str =
        "beam:transform:combine_per_key_precombine:v1";
    pub const COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN: &str =
        "beam:transform:combine_per_key_merge_accumulators:v1";
    pub const COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN: &str =
        "beam:transform:combine_per_key_extract_outputs:v1";
    pub const COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS_TRANSFORM_URN: &str =
        "beam:transform:combine_per_key_convert_to_accumulators:v1";
    pub const COMBINE_GROUPED_VALUES_TRANSFORM_URN: &str =
        "beam:transform:combine_grouped_values:v1";

    pub const COMBINE_COMPONENTS: &[&str] = &[
        COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN,
        COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN,
        COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN,
        COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS_TRANSFORM_URN,
        COMBINE_GROUPED_VALUES_TRANSFORM_URN,
    ];

    // SplittableParDoComponents
    pub const SPLITTABLE_PAIR_WITH_RESTRICTION_URN: &str =
        "beam:transform:sdf_pair_with_restriction:v1";
    pub const SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN: &str =
        "beam:transform:sdf_truncate_sized_restrictions:v1";

    pub const SPLITTABLE_PARDO_COMPONENTS: &[&str] = &[
        SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
        SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN,
    ];

    pub const SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN: &str =
        "beam:transform:sdf_split_and_size_restrictions:v1";
    pub const SPLITTABLE_PROCESS_ELEMENTS_URN: &str = "beam:transform:sdf_process_elements:v1";
    pub const SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN: &str =
        "beam:transform:sdf_process_sized_elements_and_restrictions:v1";

    //Deprecated
    pub const SPLITTABLE_PROCESS_KEYED_URN: &str = "beam:transform:sdf_process_keyed_elements:v1";

    pub const VALID_MAIN_INPUT_URNS: &[&str] = &[
        PAR_DO_TRANSFORM,
        SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
        SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
        SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN,
        SPLITTABLE_PROCESS_ELEMENTS_URN,
        SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
    ];

    pub const VALID_SIDE_INPUT_URNS: &[&str] = &[
        PAR_DO_TRANSFORM,
        SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
        SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
        SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
    ];
}
