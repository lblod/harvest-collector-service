export const cronFrequency = process.env.RECONCILIATION_CRON_PATTERN || '0 18 * * *';
export const deleteBatchSize = parseInt(process.env.DELETE_BATCH_SIZE) || 1000;
