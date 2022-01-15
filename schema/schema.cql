CONSISTENCY QUORUM;

CREATE KEYSPACE IF NOT EXISTS Blog
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

USE Blog;

CREATE TABLE Comments_By_Post (
  postId int,
  authorId int,
  authorName varchar,
  createdAt timestamp,
  commentId int,
  content varchar,
  PRIMARY KEY ((postId), createdAt, commentId)
  WITH CLUSTERING ORDER BY (createdAt DESC)
);

CREATE TABLE Users (
  userId uuid,
  name varchar,
  password varchar,
  email varchar,
  age int,
  PRIMARY KEY(userId)
);

CREATE TABLE Comments_by_author (
  postId int,
  authorId int,
  createdAt timestamp,
  commentId int,
  content varchar,
  PRIMARY KEY ((authorId), createdAt, commentId)
  WITH CLUSTERING ORDER BY (createdAt DESC)
);

CREATE TABLE Posts_by_author (
  postId uuid,
  postContent varchar,
  createdAt timestamp,
  authorId uuid,
  name varchar,
  PRIMARY KEY ((authorId), createdAt, postId)
  WITH CLUSTERING ORDER BY (createdAt DESC)
);

CREATE TABLE Posts_by_category (
  categoryName varchar,
  postId uuid,
  postContent varchar,
  createdAt timestamp,
  authorId uuid,
  authorName varchar,
  PRIMARY KEY ((categoryName), createdAt, postId)
  WITH CLUSTERING ORDER BY (createdAt DESC)
);

CREATE TABLE PostLikes (
  postId uuid,
  postLikesCounter counter,
  PRIMARY KEY ((postId))
);

CREATE TABLE LikedPostByUser (
  postId int,
  userId int,
  PRIMARY KEY ((userId), postId)
);




