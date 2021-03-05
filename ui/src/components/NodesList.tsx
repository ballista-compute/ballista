import React from "react";
import {Box } from "@chakra-ui/react";
import {Column, DateCell, DataTable} from "./DataTable";

export interface NodeInfo {
  uuid: string;
  host: string;
  port: number;
  started: string;
}

export interface NodesListProps {
  nodes?: NodeInfo[];
}

const columns : Column<any>[] = [
  {
    Header: "Node",
    accessor: "uuid",
  },
  {
    Header: "Host",
    accessor: "host",
  },
  {
    Header: "Port",
    accessor: "port",
  },
  {
    Header: "Started",
    accessor: "started",
    Cell: DateCell,
  },
];

export const NodesList: React.FunctionComponent<NodesListProps> = ({
  nodes = [],
}) => {
  return (
    <Box flex={1}>
      <DataTable maxW={960} columns={columns} data={nodes} pageSize={4} />
    </Box>
  );
};
