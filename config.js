export const CRON_FREQUENCY = process.env.CRON_FREQUENCY || '*/5 * * * *';
export const DELETE_BATCH_SIZE = parseInt(process.env.DELETE_BATCH_SIZE) || 1000;
export const SCHEDULED_TASK_CREATOR = process.env.SCHEDULED_TASK_CREATOR  || 'http://lblod.data.gift/services/migrations';