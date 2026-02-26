.PHONY: publish-web web-build

web-build:
	cd web && npm ci && npm run build

publish-web: web-build
	$(eval DATE := $(shell date +%Y-%m-%d))
	python3 generate_charts.py build
	cp build/$(DATE)/index.html index.html
	@git add build index.html
	@git commit -m "Publish results for $(DATE)"
	@git push
