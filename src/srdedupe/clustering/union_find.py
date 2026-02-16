"""Union-Find (Disjoint Set Union) data structure for clustering."""


class UnionFind:
    """Union-Find data structure with path compression and union by rank.

    Implements the classic DSU algorithm for efficiently finding connected
    components in a graph.

    Attributes
    ----------
    parent : dict[str, str]
        Parent pointers for each element.
    rank : dict[str, int]
        Rank (approximate tree height) for each root.
    """

    def __init__(self) -> None:
        """Initialize empty Union-Find structure."""
        self.parent: dict[str, str] = {}
        self.rank: dict[str, int] = {}

    def make_set(self, x: str) -> None:
        """Create a new set containing element x.

        Parameters
        ----------
        x : str
            Element to add.
        """
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        """Find root of set containing x with path compression.

        Parameters
        ----------
        x : str
            Element to find.

        Returns
        -------
        str
            Root of set containing x.
        """
        if x not in self.parent:
            self.make_set(x)

        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])

        return self.parent[x]

    def union(self, x: str, y: str) -> None:
        """Union sets containing x and y using union by rank.

        Parameters
        ----------
        x : str
            First element.
        y : str
            Second element.
        """
        root_x = self.find(x)
        root_y = self.find(y)

        if root_x == root_y:
            return

        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1

    def get_components(self) -> list[list[str]]:
        """Get all connected components.

        Returns
        -------
        list[list[str]]
            List of components, each component is a list of elements.
        """
        components_dict: dict[str, list[str]] = {}

        for element in self.parent:
            root = self.find(element)
            if root not in components_dict:
                components_dict[root] = []
            components_dict[root].append(element)

        return list(components_dict.values())
