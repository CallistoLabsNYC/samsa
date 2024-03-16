mod testsupport;

use random_word::Lang;
use samsa::prelude::redpanda::adminapi::AdminAPI;
use samsa::prelude::Error;

#[tokio::test]
async fn it_returns_404_when_deleting_non_existing() -> Result<(), Box<Error>> {
    let (skip, urls) = testsupport::get_broker_urls()?;
    if skip {
        return Ok(());
    }
    let client = AdminAPI::builder().urls(urls).build()?;
    let name = random_word::gen(Lang::En);
    let res = client.delete_wasm_transform(name).await;
    assert!(res.is_err());
    assert_eq!(res.err().unwrap(), Error::NotFound);
    Ok(())
}
