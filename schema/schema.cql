CONSISTENCY QUORUM;

CREATE KEYSPACE IF NOT EXISTS Blog
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

USE Blog;

CREATE TABLE Comments_By_Post (
  postId uuid,
  authorId uuid,
  authorName varchar,
  createdAt timestamp,
  commentId uuid,
  commentContent varchar,
  PRIMARY KEY ((postId), createdAt, commentId)
)
  WITH CLUSTERING ORDER BY (createdAt DESC);

CREATE TABLE Users (
  userId uuid,
  name varchar,
  password varchar,
  email varchar,
  age int,
  PRIMARY KEY(userId)
);

CREATE TABLE Comments_by_author (
  postId uuid,
  authorId uuid,
  createdAt timestamp,
  commentId uuid,
  commentContent varchar,
  PRIMARY KEY ((authorId), createdAt, commentId)
)
  WITH CLUSTERING ORDER BY (createdAt DESC);

CREATE TABLE Posts_by_author (
  postId uuid,
  postContent varchar,
  createdAt timestamp,
  authorId uuid,
  authorName varchar,
  PRIMARY KEY ((authorId), createdAt, postId)
)
  WITH CLUSTERING ORDER BY (createdAt DESC);

CREATE TABLE Posts_by_category (
  categoryName varchar,
  postId uuid,
  postContent varchar,
  createdAt timestamp,
  authorId uuid,
  authorName varchar,
  PRIMARY KEY ((categoryName), createdAt, postId)
)
  WITH CLUSTERING ORDER BY (createdAt DESC);

CREATE TABLE PostLikes (
  postId uuid,
  postLikesCounter counter,
  PRIMARY KEY ((postId))
);

CREATE TABLE LikedPostByUser (
  postId uuid,
  userId uuid,
  PRIMARY KEY ((userId), postId)
);




