import { Dashboard } from "@/components/Dashboard";

export default function Page() {
  return <Dashboard githubRepoUrl={process.env.NEXT_PUBLIC_GITHUB_REPO_URL ?? null} />;
}
