from rest_framework.views import APIView
from rest_framework.response import Response
from .services import create_job
from .models import Job


class JobAPI(APIView):

    def post(self, request):

        job = create_job(
            name=request.data.get("name"),
            payload=request.data.get("payload")
        )

        return Response({
            "job_id": job.id,
            "status": job.status
        })


    def get(self, request, job_id):

        job = Job.objects.get(id=job_id)

        return Response({
            "job_id": job.id,
            "status": job.status,
            "result": job.result,
            "error": job.error,
            "retries": job.retries
        })