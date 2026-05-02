#[macro_export]
macro_rules! build_main {
    () => {
        fn main() {
            ::taskflow_build::run_with_default();
        }
    };
    (env = $env_key:literal) => {
        fn main() {
            ::taskflow_build::run_with_env($env_key);
        }
    };
}