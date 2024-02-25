use futures::stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<samsa::prelude::Error>> {
    let bootstrap_addrs = vec!["127.0.0.1:9092".to_string()];
let partitions = vec![0];
let topic_name = "my-topic";
let assignment = std::collections::HashMap::from([(topic_name.to_string(), partitions)]);
let group_id = "The Data Boyz".to_string();

let consumer_group_member = samsa::prelude::ConsumerGroupBuilder::new(
    bootstrap_addrs,
    group_id,
    assignment,
).await?
.build().await?;
 
let stream = consumer_group_member.into_stream();
// have to pin streams before iterating
tokio::pin!(stream);
 
// Stream will do nothing unless consumed.
while let Some(batch) = stream.next().await {
    println!("{:?}", batch);
}

    Ok(())
}
