from rest_framework.views import APIView
from rest_framework.response import Response
from .services import create_job


class CreateJobAPI(APIView):
    def post(self,request):
        job=create_job(
            name=request.data.get("name"),
            payload=request.data.get("payload")
        )

        return Response({
            "job_id" : job.id,
            "status": job.status
        })