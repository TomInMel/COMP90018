module.exports = async function (context) {
  const req = context.request;

  let bodyBuffer = req.body;

  let body;
  try {
    if (Buffer.isBuffer(bodyBuffer)) {
      body = JSON.parse(bodyBuffer.toString("utf8"));
    } else if (typeof bodyBuffer === "string") {
      body = JSON.parse(bodyBuffer);
    } else {
      body = bodyBuffer;
    }
  } catch (e) {
    console.error("Invalid JSON in request body:", e);
    return { status: 400, body: "Invalid JSON" };
  }

  const { type, query, subreddit, data } = body;
  let resultDoc;

  if (type === "post") {
    resultDoc = {
      type: "post",
      id: `reddit_post_${data.id}`,
      title: data.title,
      platform: "reddit",
      content: data.content,
      created_utc: data.created_utc,
      author: data.author,
      num_comments: data.num_comments,
      like: data.score,
    };
  } else if (type === "comment") {
    resultDoc = {
      type: "comment",
      id: `reddit_comment_${data.id}`,
      platform: "reddit",
      post_id: `reddit_post_${data.post_id}`,
      content: data.body,
      created_utc: data.created_utc,
      author: data.author,
      like: data.score,
    };
  } else {
    console.error("Unknown payload type:", type);
    return { status: 400, body: `Unknown type: ${type}` };
  }

  console.log(
    `Processed ${type} | query: ${query} | subreddit: ${subreddit}` +
      ` | returned 1 document`
  );

  return {
    status: 200,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(resultDoc, null, 2),
  };
};
