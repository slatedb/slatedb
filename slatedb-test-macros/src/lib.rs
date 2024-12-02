use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, AttributeArgs, ItemFn, ReturnType};

// A macro that runs a test (via `tokio::test`) after installing a signal
// handler that will print backtraces for all threads via gdb if the test
// is interrupted by SIGTERM. Useful for debugging tests that hang in CI.
#[proc_macro_attribute]
pub fn test(tokio_test_attrs: TokenStream, test_fn: TokenStream) -> TokenStream {
    let tokio_test_attrs = parse_macro_input!(tokio_test_attrs as AttributeArgs);
    let test_fn = parse_macro_input!(test_fn as ItemFn);

    let test_fn_attrs = &test_fn.attrs;
    let test_fn_body = &test_fn.block;
    let test_fn_name = &test_fn.sig.ident;
    let test_fn_ret = match &test_fn.sig.output {
        ReturnType::Default => quote! {},
        ReturnType::Type(_, type_) => quote! {-> #type_},
    };

    quote! {
      #[tokio::test(#(#tokio_test_attrs)*)]
      #(#test_fn_attrs)*
      async fn #test_fn_name() #test_fn_ret {
        async fn test_impl() #test_fn_ret {
          #test_fn_body
        }

        let sigterm_handler_task = ::std::env::var_os("CI").is_some().then(|| {
          let mut signal = ::tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("test harness failed to create SIGTERM handler");

          ::tokio::spawn(async move {
            signal.recv().await;
            eprintln!("SIGTERM received; collecting backtraces from running threads with gdb...");
            let pid = std::process::id();
            ::std::process::Command::new("sudo")
              .arg("gdb")
              .arg("-p")
              .arg(pid.to_string())
              .arg("-ex")
              .arg("thread apply all bt")
              .spawn()
              .expect("failed to spawn gdb")
              .wait()
              .expect("gdb failed");
            std::process::abort();
          })
        });

        let res = test_impl().await;
        sigterm_handler_task.map(|task| task.abort());
        res
      }
    }
    .into()
}
