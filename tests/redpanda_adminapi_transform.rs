mod testsupport;

use random_word::Lang;
use samsa::prelude::redpanda::adminapi::AdminAPI;
use samsa::prelude::Error;

#[tokio::test]
async fn it_cannot_delete_non_existing() -> Result<(), Box<Error>> {
    let (skip, urls) = testsupport::get_broker_urls()?;
    if skip {
        return Ok(());
    }
    let _client = AdminAPI::builder().urls(urls).build()?;

    let _name = random_word::gen(Lang::En);
    // TODO

    Ok(())
}
