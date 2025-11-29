# Docker Setup Guide

## Initial Setup

To start the distributed database system with all three nodes, run:

```bash
docker-compose down
docker-compose down -v
docker-compose up -d
```

## Resetting Database State During CRUD Testing

**Important**: During CRUD testing, you may need to reset the database back to its initial state. To do this, run all three commands together:

```bash
docker-compose down
docker-compose down -v
docker-compose up -d
```

### What these commands do:

1. **`docker-compose down`** - Stops and removes all containers, networks created by docker-compose
2. **`docker-compose down -v`** - Removes the containers AND deletes all volumes (this wipes the database data)
3. **`docker-compose up -d`** - Starts all services in detached mode and reinitializes the databases with the init scripts

This sequence ensures a clean slate by removing all existing data and recreating the databases with their initial configuration.
