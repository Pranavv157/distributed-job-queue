def publish(event):
    #raise Exception("broker down")
        from jobs.tasks import process_job_completed_event
        if event.event_type == "job_completed":

         process_job_completed_event.delay(
             event.payload
         )