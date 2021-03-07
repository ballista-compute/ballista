import React from "react";
import {Box, Grid, VStack} from "@chakra-ui/react";
import {Header} from "./components/Header";
import {ClusterProps, Summary} from "./components/Summary";
import "./App.css";
import {NodeInfo, NodeStatus} from "./components/NodesList";
import {QueriesList, Query, QueryStatus} from "./components/QueriesList";
import {Footer} from "./components/Footer";

function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
    var r = (Math.random() * 16) | 0,
      v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function ip() {
  return (
    Math.floor(Math.random() * 255) +
    1 +
    "." +
    Math.floor(Math.random() * 255) +
    "." +
    Math.floor(Math.random() * 255) +
    "." +
    Math.floor(Math.random() * 255)
  );
}

const getRandomNodes = (num: number): NodeInfo[] => {
  const nodes: NodeInfo[] = [];

  for (let i = 0; i < num; i++) {
    nodes.push({
      started: new Date().toISOString(),
      host: ip(),
      port: 8080,
      status: NodeStatus.RUNNING,
      uuid: uuidv4(),
    });
  }
  return nodes;
};

const getRandomQueries = (num: number): Query[] => {
  const nodes: Query[] = [];

  for (let i = 0; i < num; i++) {
    nodes.push({
      started: new Date().toISOString(),
      query: "SELECT \n" +
          "    employee.id,\n" +
          "    employee.first_name,\n" +
          "    employee.last_name,\n" +
          "    SUM(DATEDIFF(\"SECOND\", call.start_time, call.end_time)) AS call_duration_sum\n" +
          "FROM call\n" +
          "INNER JOIN employee ON call.employee_id = employee.id\n" +
          "GROUP BY\n" +
          "    employee.id,\n" +
          "    employee.first_name,\n" +
          "    employee.last_name\n" +
          "ORDER BY\n" +
          "    employee.id ASC;",
      status: QueryStatus.RUNNING,
      progress: Math.round(Math.random() * 100),
      uuid: uuidv4()
    });
  }
  return nodes;
};

const cluster: ClusterProps = {
  clusterInfo: {
    started: new Date().toISOString(),
    status: "Active",
    version: "1.0.4",
  },
  nodes: { nodes: getRandomNodes(5) },
};

const queries = getRandomQueries(17);

function App() {
  return (
    <Box>
      <Grid minH="100vh">
        <VStack alignItems={"flex-start"} spacing={0} width={"100%"}>
          <Header {...cluster.clusterInfo} />
          <Summary clusterInfo={cluster.clusterInfo} nodes={cluster.nodes} />
          <QueriesList queries={queries} />
          <Footer />
        </VStack>
      </Grid>
    </Box>
  );
}

export default App;
