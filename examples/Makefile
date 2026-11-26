EXAMPLES := ollama langchain-golang cloudevents dapr kubernetes nats kafka watermill
COMPOSE ?= ./run.sh

.PHONY: validate hello hello-agent $(addprefix up-,$(EXAMPLES)) $(addprefix logs-,$(EXAMPLES)) $(addprefix down-,$(EXAMPLES))

# Variant A: Ollama chat client only (chatmodels.Resolve + partners/ollama).
# Do not use --abort-on-container-exit here: the builder exits first by design.
hello:
	@$(COMPOSE) -f ollama/compose.yaml up -d && \
		$(COMPOSE) -f ollama/compose.yaml wait hello; rc=$$?; \
		$(COMPOSE) -f ollama/compose.yaml logs hello; \
		$(COMPOSE) -f ollama/compose.yaml down; \
		exit $$rc

# Variant B: full LangGraph Go runtime (prebuilt.CreateReactAgent + saver).
hello-agent:
	@$(COMPOSE) -f langchain-golang/compose.yaml up -d agent-hello && \
		$(COMPOSE) -f langchain-golang/compose.yaml wait agent-hello; rc=$$?; \
		$(COMPOSE) -f langchain-golang/compose.yaml logs agent-hello; \
		$(COMPOSE) -f langchain-golang/compose.yaml down; \
		exit $$rc

validate:
	@for d in $(EXAMPLES); do \
		$(COMPOSE) -f $$d/compose.yaml config --quiet || exit 1; \
		echo "$$d OK"; \
	done

$(addprefix up-,$(EXAMPLES)): up-%:
	$(COMPOSE) -f $*/compose.yaml up -d

$(addprefix logs-,$(EXAMPLES)): logs-%:
	$(COMPOSE) -f $*/compose.yaml logs -f

# Never pass -v: the Ollama model volume is shared across all groups.
$(addprefix down-,$(EXAMPLES)): down-%:
	$(COMPOSE) -f $*/compose.yaml down
