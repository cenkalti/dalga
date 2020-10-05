# Changelog

## [4.0.0] - 2020-10-05

- Added endpoint for listing jobs.
- Needs database migration:
```
ALTER TABLE dalga ADD COLUMN `id` BIGINT NOT NULL AUTO_INCREMENT UNIQUE
```
