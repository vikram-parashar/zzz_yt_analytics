export async function GET() {
  console.log("MOTHERDUCK_TOKEN:", process.env.MOTHERDUCK_TOKEN);
  return Response.json({
    exists: !!process.env.MOTHERDUCK_TOKEN,
  });
}