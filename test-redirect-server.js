
import express from "express";

const app = express();

app.get("/redirect", (req, res) => {
  console.log("Redirect endpoint was reached");

  res.redirect(302, "http://127.0.0.1:4000/health");
});

app.listen(4500, "127.0.0.1", () => {
  console.log("Redirect test server running on port 4500");
});
