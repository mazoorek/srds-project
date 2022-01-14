CONSISTENCY QUORUM;

CREATE KEYSPACE IF NOT EXISTS Blog
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

USE Blog;

CREATE TABLE Comments (
  postId int,
  authorId int,
  commentId int,
  content varchar,
  PRIMARY KEY ((postId), authorId, commentId)
);

CREATE TABLE Users (
  userId uuid,
  name varchar,
  surname varchar,
  age int,
  PRIMARY KEY(userId)
);

CREATE TABLE Posts (
  postId int,
  content varchar,
  authorId int,
  PRIMARY KEY ((authorId), postId)
);

CREATE TABLE PostLikes (
  authorId int,
  postId int,
  postLikesCounter counter,
  PRIMARY KEY ((authorId, postId))
);

-- CREATE TABLE Likes (
--   contentType varchar,
--   likedContentId int,
--   userId int,
--   PRIMARY KEY ((contentType, likedContentId, userId))
-- );

CREATE TABLE LikedPostByUser (
  authorId int,
  postId int,
  userId int,
  PRIMARY KEY ((authorId, postId, userId))
);




