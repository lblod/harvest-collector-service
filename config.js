export const CRON_FREQUENCY = process.env.CRON_FREQUENCY || '*/5 * * * *';
export const ALLOW_CRON_JOB = process.env.ALLOW_CRON_JOB || false;
export const SCHEDULED_TASK_CREATOR = process.env.SCHEDULED_TASK_CREATOR  || 'http://lblod.data.gift/services/migrations';
