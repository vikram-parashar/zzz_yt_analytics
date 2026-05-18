import { TopAgent } from "@/lib/types";
import { formatNumber } from "@/lib/data";

interface AgentTableProps {
  agents: TopAgent[];
}

const ATTRIBUTE_COLORS: Record<string, string> = {
  Ice: "badge-info",
  Electric: "badge-warning",
  Fire: "badge-error",
  Ether: "badge-secondary",
  Physical: "badge-accent",
};

export default function AgentTable({ agents }: AgentTableProps) {
  return (
    <div className="card bg-base-200 shadow-sm">
      <div className="card-body">
        <h3 className="card-title text-sm">Top Agents by Views</h3>
        <div className="overflow-x-auto mt-2">
          <table className="table table-sm">
            <thead>
              <tr>
                <th>#</th>
                <th>Agent</th>
                <th>Rank</th>
                <th>Attribute</th>
                <th className="text-right">Views</th>
                <th className="text-right">Likes</th>
                <th className="text-right">Videos</th>
              </tr>
            </thead>
            <tbody>
              {agents.map((agent, i) => (
                <tr key={agent.name} className="hover">
                  <td className="opacity-50">{i + 1}</td>
                  <td className="font-medium">{agent.name}</td>
                  <td>
                    <span
                      className={`badge badge-xs ${
                        agent.rank === "S" ? "badge-warning" : "badge-ghost"
                      }`}
                    >
                      {agent.rank}
                    </span>
                  </td>
                  <td>
                    <span
                      className={`badge badge-xs ${
                        ATTRIBUTE_COLORS[agent.attribute] || "badge-ghost"
                      }`}
                    >
                      {agent.attribute}
                    </span>
                  </td>
                  <td className="text-right font-mono text-sm">
                    {formatNumber(agent.total_views)}
                  </td>
                  <td className="text-right font-mono text-sm">
                    {formatNumber(agent.total_likes)}
                  </td>
                  <td className="text-right font-mono text-sm">
                    {agent.video_count}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

