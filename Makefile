APP := application-3-tradehub
NS  := tradehub
GW  := tradehub-api-gateway

.PHONY: up
up:
	docker compose up --build -d

.PHONY: down
down:
	docker compose down -v

.PHONY: logs
logs:
	docker compose logs -f

.PHONY: logs-svc
logs-svc:
	docker compose logs -f $(SVC)

.PHONY: minikube-build
minikube-build:
	eval $$(minikube docker-env) && \
	for svc in api-gateway trader-service trading-account-service order-management-service \
	           execution-engine clearing-service portfolio-service market-data-service notification-service; do \
	  docker build -t tradehub/$$svc:latest tradehub/services/$$svc/; \
	done

.PHONY: k8s-deploy
k8s-deploy: minikube-build
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/config.yaml
	kubectl apply -f k8s/infra.yaml
	kubectl apply -f k8s/deployments.yaml
	kubectl apply -f istio/peer-authentication.yaml
	kubectl apply -f istio/destination-rule.yaml
	kubectl apply -f istio/gateway.yaml
	kubectl apply -f istio/virtual-service.yaml
	kubectl apply -f istio/telemetry.yaml
	kubectl apply -f observability/prometheus.yaml
	kubectl wait --for=condition=ready pod -l app=$(GW) -n $(NS) --timeout=120s
	@echo "TradeHub running in Minikube"

.PHONY: k8s-status
k8s-status:
	kubectl get pods -n $(NS) -o wide

.PHONY: k8s-logs
k8s-logs:
	kubectl logs -n $(NS) -l app=$(GW) -c istio-proxy -f --tail=50

.PHONY: k8s-logs-svc
k8s-logs-svc:
	kubectl logs -n $(NS) -l app=$(SVC) -c istio-proxy -f --tail=50

.PHONY: dashboard-kiali
dashboard-kiali:
	istioctl dashboard kiali

.PHONY: k8s-clean
k8s-clean:
	kubectl delete namespace $(NS) --ignore-not-found

.PHONY: clean
clean: down k8s-clean
