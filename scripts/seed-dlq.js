import amqplib from 'amqplib';
import { randomUUID } from 'node:crypto';
import { config } from '../src/config.js';

const TENANT_ID = 'rcruqwmnrsse1imyk50lww';
const COMPANY_ID = '1222';
const STORE_ID = '35406';

const CHANNELS = {
  TIKTOK: 1,
};

const seeds = [
  {
    queue: 'tiktok-order-batch-queue-dead-letters',
    payload: {
      data: {
        tenantId: TENANT_ID,
        companyId: COMPANY_ID,
        storeId: STORE_ID,
        channelId: CHANNELS.TIKTOK,
        refNos: ['580060897126679570'],
      },
    },
  },
  {
    queue: 'shopee-order-batch-queue-dead-letters',
    payload: {
      data: {
        tenantId: TENANT_ID,
        companyId: COMPANY_ID,
        storeId: STORE_ID,
        type: 'order',
        refNos: ['250820AS0BRN99'],
      },
    },
  },
];

async function main() {
  const connection = await amqplib.connect(config.rabbitmqUrl);
  const channel = await connection.createConfirmChannel();

  for (const seed of seeds) {
    const body = Buffer.from(JSON.stringify(seed.payload));
    channel.sendToQueue(seed.queue, body, {
      messageId: randomUUID(),
      contentType: 'application/json',
    });
    console.log(`seeded "${seed.queue}" with refNos ${seed.payload.data.refNos.join(', ')}`);
  }

  await channel.waitForConfirms();
  await channel.close();
  await connection.close();
  console.log(`done — seeded ${seeds.length} message(s)`);
}

main().catch((err) => {
  console.error('failed to seed DLQs:', err.message);
  process.exitCode = 1;
});
