FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /pageobserver ./cmd/pageobserver
RUN mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /pageobserver /app/pageobserver
COPY --from=build --chown=65532:65532 /data /app/data
VOLUME ["/app/data"]
EXPOSE 8080
ENTRYPOINT ["/app/pageobserver"]
CMD ["-addr", "0.0.0.0:8080", "-db", "/app/data/observer.db"]
