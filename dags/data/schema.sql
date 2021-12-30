DROP DATABASE IF EXISTS "meme";

CREATE DATABASE "meme";

USE "meme";

CREATE TABLE "meme_fact" (
  "id" BIGSERIAL PRIMARY KEY,
  "title" varchar(255) NOT NULL,
  "url" varchar(255) NOT NULL,
  "template_url" varchar(255),
  "description" varchar(255),
  "children_count" bigint unsigned,
  "website_count" bigint unsigned,
  "date_id" bigint unsigned,
  "parent_id" bigint unsigned,
  "child_id" bigint unsigned,
  "origin_id" bigint unsigned,
  "keyword_id" bigint unsigned,
  "tag_id" bigint unsigned
);

CREATE TABLE "date_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "date" Date,
  "month_number" int,
  "month_name" varchar(255),
  "year" int
);

CREATE TABLE "child_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "child_link" varchar(255) NOT NULL UNIQUE
);

CREATE TABLE "tag_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "text" varchar(255) NOT NULL UNIQUE
);

CREATE TABLE "keyword_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "keyword" varchar(255) NOT NULL UNIQUE
);

CREATE TABLE "origin_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "origin" varchar(255) NOT NULL UNIQUE
);

CREATE TABLE "parent_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "parent_link" varchar(255) NOT NULL UNIQUE
);

ALTER TABLE "meme_fact" ADD FOREIGN KEY ("keyword_id") REFERENCES "keyword_dim" ("id");

ALTER TABLE "meme_fact" ADD FOREIGN KEY ("origin_id") REFERENCES "origin_dim" ("id");

ALTER TABLE "meme_fact" ADD FOREIGN KEY ("parent_id") REFERENCES "parent_dim" ("id");

ALTER TABLE "meme_fact" ADD FOREIGN KEY ("date_id") REFERENCES "date_dim" ("id");

ALTER TABLE "meme_fact" ADD FOREIGN KEY ("child_id") REFERENCES "child_dim" ("id");

ALTER TABLE "meme_fact" ADD FOREIGN KEY ("tag_id") REFERENCES "tag_dim" ("id");


CREATE INDEX ixfk__meme_fact__keyword ON "meme_fact" (keyword_id ASC);

CREATE INDEX ixfk__meme_fact__origin ON "meme_fact" (origin_id ASC);

CREATE INDEX ixfk__meme_fact__parent ON "meme_fact" (parent_id ASC);

CREATE INDEX ixfk__meme_fact__date ON "meme_fact" (date_id ASC);

CREATE INDEX ixfk__meme_fact__child ON "meme_fact" (child_id ASC);

CREATE INDEX ixfk__meme_fact__tag ON "meme_fact" (tag_id ASC);

