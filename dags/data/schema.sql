DROP TABLE IF EXISTS "meme_fact";
DROP TABLE IF EXISTS "date_dim";
DROP TABLE IF EXISTS "child_dim";
DROP TABLE IF EXISTS "tag_dim";
DROP TABLE IF EXISTS "keyword_dim";
DROP TABLE IF EXISTS "origin_dim";
DROP TABLE IF EXISTS "parent_dim";

CREATE TABLE "meme_fact" (
  "id" BIGSERIAL PRIMARY KEY,
  "title" varchar(255) NOT NULL,
  "url" varchar(255) NOT NULL,
  "label" varchar(255),
  "description" text,
  "children_count" bigint,
  "tags_count" bigint,
  "keywords_count" bigint,
  "adult" varchar(255),
  "spoof" varchar(255),
  "medical" varchar(255),
  "violence" varchar(255),
  "racy" varchar(255),
  "date_id" bigint,
  "parent_id" bigint,
  "child_id" bigint,
  "origin_id" bigint,
  "keyword_id" bigint,
  "tag_id" bigint
);

CREATE TABLE "date_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "date" Date UNIQUE,
  "month_number" int,
  "month_name" varchar(255),
  "year" int
);

CREATE TABLE "child_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "text" varchar(255) NOT NULL UNIQUE
);

CREATE TABLE "tag_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "text" varchar(255) NOT NULL UNIQUE
);

CREATE TABLE "keyword_dim" (
  "id" BIGSERIAL PRIMARY KEY,
  "text" varchar(255) NOT NULL UNIQUE
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
