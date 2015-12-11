CREATE USER "play" NOSUPERUSER INHERIT CREATEROLE;

CREATE DATABASE mentions WITH OWNER = "play" ENCODING 'UTF8';

GRANT ALL PRIVILEGES ON DATABASE mentions to "play";

\connect mentions play

CREATE TABLE twitter_user (
  id bigserial primary key,
  created_on timestamp with time zone NOT NULL,
  twitter_user_name varchar NOT NULL
);

CREATE TABLE mentions (
  id bigserial primary key,
  tweet_id varchar NOT NULL,
  user_id bigint NOT NULL,
  created_on timestamp with time zone NOT NULL,
  text varchar NOT NULL
);
