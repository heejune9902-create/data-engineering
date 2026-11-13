# Docker 서비스 실행

HTTP 서비스를 컨테이너로 빌드하고 실행하는 기본 예제다.

```bash
docker build -t docker-service-example .
docker run --rm -p 127.0.0.1:8081:8080 docker-service-example
```

`http://localhost:8081`에서 응답을 확인한다.
