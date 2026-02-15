from extract.pull_agents_html import pull_agents_html
from load.load_agents import load_agents_func
from transform.parse_agents import parse_agents_func


def scrape_agent_func():
    pull_agents_html()
    parse_agents_func()
    load_agents_func()


if __name__ == "__main__":
    scrape_agent_func()
