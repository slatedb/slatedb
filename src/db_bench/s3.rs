pub(crate) fn load_aws_creds() -> (String, String) {
    let aws_key = std::env::var("AWS_ACCESS_KEY_ID").expect("must supply AWS access key");
    let aws_secret = std::env::var("AWS_SECRET_ACCESS_KEY").expect("must supply AWS secret");
    (aws_key, aws_secret)
}
