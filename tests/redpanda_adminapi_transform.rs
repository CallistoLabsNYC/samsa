mod testsupport;

use random_word::Lang;
use samsa::prelude::redpanda::adminapi::{AdminAPI, TransformMetadata};
use samsa::prelude::Error;

#[tokio::test]
async fn it_returns_not_found_when_deleting_non_existing() -> Result<(), Box<Error>> {
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

#[tokio::test]
async fn it_can_write_then_read() -> Result<(), Error> {
    let (skip, urls) = testsupport::get_broker_urls()?;
    if skip {
        return Ok(());
    }
    let (skip, topic) = testsupport::get_topic()?;
    if skip {
        return Ok(());
    }
    let client = AdminAPI::builder().urls(urls).build()?;

    // Create
    let name = random_word::gen(Lang::En);
    let transform_metadata = TransformMetadata {
        name: name.to_string(),
        input_topic: topic.clone(),
        output_topics: topic,
        ..Default::default()
    };
    let contents =
        std::fs::read("testdata/add_one.wasm").map_err(|err| Error::ArgError(err.to_string()))?;
    let res = client
        .clone()
        .deploy_wasm_transform(transform_metadata, contents)
        .await?;

    // List
    // TODO

    // Delete
    client.delete_wasm_transform(name).await?;

    // List
    // TODO

    Ok(())
}
