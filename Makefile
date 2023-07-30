SHELL := /bin/bash

graphile-migrate-watch:
	source .env.local && graphile-migrate watch
