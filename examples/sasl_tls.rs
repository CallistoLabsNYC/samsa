use samsa::prelude::{
    BrokerAddress, ConsumerBuilder, SaslConfig, SaslTlsConfig, SaslTlsConnection,
    TlsConnectionOptions, TopicPartitionsBuilder,
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), ()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();

    let tls_config = TlsConnectionOptions {
        broker_options: vec![BrokerAddress {
            host: "piggy.callistolabs.cloud".to_owned(),
            port: 9092,
        }],
        key: "./etc/redpanda/certs/piggy.key".into(),
        cert: "./etc/redpanda/certs/piggy_callisto_labs_cloud.crt".into(),
        cafile: Some("./etc/redpanda/certs/trustedroot.crt".into()),
    };

    let sasl_config = SaslConfig::new(String::from("myuser"), String::from("pass1234"), None, None);

    let options = SaslTlsConfig {
        tls_config,
        sasl_config,
    };

    let topic_name = "atopic";

    let s = ConsumerBuilder::<SaslTlsConnection>::new(
        options,
        TopicPartitionsBuilder::new()
            .assign(topic_name.to_owned(), vec![0])
            .build(),
    )
    .await
    .unwrap()
    .build()
    .into_stream();

    tokio::pin!(s);

    while let Some(m) = s.next().await {
        tracing::info!("{:?} messages read", m.unwrap().count());
    }

    Ok(())
}
