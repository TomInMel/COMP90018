module.exports = async function (context) {
  const data = context.request.body;
  let processedData;

  if (data.type === "comment") {
    processedData = {
      type: "comment",
      id: data.cid,
      post_id: data.post_id,
      content: data.record && data.record.text ? data.record.text : "",
      created_utc: data.record && data.record.createdAt ? data.record.createdAt : "",
      author: data.author
        ? (data.author.displayName || data.author.handle || "Unknown")
        : "Unknown",
      like: data.likeCount || 0,
    };

  } else if (data.type === "post") {

    processedData = {
      type: "post",
      id: data.cid,
      title: "",
      platform: "bluesky",
      content: data.record && data.record.text ? data.record.text : "",
      created_utc: data.record && data.record.createdAt ? data.record.createdAt : "",
      author: data.author ? (data.author.displayName || data.author.handle || "Unknown") : "Unknown",
      num_comments: data.replyCount || 0,
      like: data.likeCount || 0,
    };
    
  } else {
    return {
      status: 400,
      body: JSON.stringify({ error: "Unsupported type" })
    };
  }

  return {
    status: 200,
    body: JSON.stringify(processedData, null, 2)
  };
};