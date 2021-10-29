export class ProcessingQueue {
  constructor(name = 'Default') {
    this.name = name;
    this.queue = [];
    this.run();
    this.executing = false; //To avoid subtle race conditions TODO: is this required?
  }

  async run() {
    if (this.queue.length > 0 && !this.executing) {
      const job = this.queue.shift();
      try {
        this.executing = true;
        console.log(`${this.name}: Executing oldest task on queue`);
        await job.task();
        console.log(`${this.name}: Remaining number of tasks ${this.queue.length}`);
        this.run();
      }
      catch (error) {
        await job.onError(error);
      }
      finally {
        this.executing = false;
      }
    }
    else {
      setTimeout(() => { this.run(); }, (process.env.QUEUE_POLL_INTERVAL || 100));
    }
  }

  addJob(origin, onError = async (error) => { console.error(`${this.name}: Error while processing task`, error); }) {
    this.queue.push({
      task: origin,
      onError: onError
    });
  }
}
