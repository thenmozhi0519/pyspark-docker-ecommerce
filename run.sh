#!/bin/bash
MSYS_NO_PATHCONV=1 docker exec -it spark-local python /workspace/scripts/$1
 