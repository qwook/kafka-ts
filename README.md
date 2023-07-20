# Kafka-TS

A toy Kafka consumer written in Typescript.

## Getting Started

```bash
npm install
npx ts-node index.ts --group-id <GROUP ID> --topic <TOPIC>
```

or

```bash
yarn install
npx ts-node index.ts --group-id <GROUP ID> --topic <TOPIC>
```

## Developing

Most of the core code exists in `kafkaConsumerEngine.ts`.

Another entry point is `index.ts`.

This consumer is developed with Hot-Module-Reloading at the core. This means most changes will not need a complete restart of the consumer.
