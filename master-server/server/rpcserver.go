package server

import (
	"golang.org/x/net/context"
	"model/pkg/mspb"
)

func (service *Server) GatewayHeartbeat(ctx context.Context, req *mspb.GatewayHeartbeatRequest) (*mspb.GatewayHeartbeatResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.GatewayHeartbeatResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	resp := service.handleGatewayHeartbeat(ctx, req)
	return resp, nil
}

func (service *Server) NodeHeartbeat(ctx context.Context, req *mspb.NodeHeartbeatRequest) (*mspb.NodeHeartbeatResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.NodeHeartbeatResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	resp := service.handleNodeHeartbeat(ctx, req)
	return resp, nil
}

func (service *Server) RangeHeartbeat(ctx context.Context, req *mspb.RangeHeartbeatRequest) (*mspb.RangeHeartbeatResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.RangeHeartbeatResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	resp := service.handleRangeHeartbeat(ctx, req)
	return resp, nil
}

// ReportEvent report event to master server
func (service *Server) ReportEvent(ctx context.Context, req *mspb.ReportEventRequest) (*mspb.ReportEventResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.ReportEventResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleReportEvent(ctx, req)
}

func (service *Server) GetTopologyEpoch(ctx context.Context, req *mspb.GetTopologyEpochRequest) (*mspb.GetTopologyEpochResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.GetTopologyEpochResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleGetTopologyEpoch(ctx, req)
}

func (service *Server) GetNode(ctx context.Context, req *mspb.GetNodeRequest) (*mspb.GetNodeResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.GetNodeResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleGetNode(ctx, req)
}
func (service *Server) GetDB(ctx context.Context, req *mspb.GetDBRequest) (*mspb.GetDBResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.GetDBResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleGetDb(ctx, req)
}
func (service *Server) GetTable(ctx context.Context, req *mspb.GetTableRequest) (*mspb.GetTableResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.GetTableResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleGetTable(ctx, req)
}

func (service *Server) GetTableById(ctx context.Context, req *mspb.GetTableByIdRequest) (*mspb.GetTableByIdResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.GetTableByIdResponse{}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleGetTableById(ctx, req)
}

func (service *Server) GetColumns(ctx context.Context, req *mspb.GetColumnsRequest) (*mspb.GetColumnsResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.GetColumnsResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleGetColumns(ctx, req)
}
func (service *Server) GetColumnByName(ctx context.Context, req *mspb.GetColumnByNameRequest) (*mspb.GetColumnByNameResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.GetColumnByNameResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleGetColumnByName(ctx, req)
}
func (service *Server) GetColumnById(ctx context.Context, req *mspb.GetColumnByIdRequest) (*mspb.GetColumnByIdResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.GetColumnByIdResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleGetColumnById(ctx, req)
}
func (service *Server) GetMSLeader(ctx context.Context, req *mspb.GetMSLeaderRequest) (*mspb.GetMSLeaderResponse, error) {
	return service.handleGetMsLeader(ctx, req)
}

func (service *Server) TruncateTable(context.Context, *mspb.TruncateTableRequest) (*mspb.TruncateTableResponse, error) {
    return &mspb.TruncateTableResponse{}, nil
}

func (service *Server) AddColumn(ctx context.Context, req *mspb.AddColumnRequest) (*mspb.AddColumnResponse, error) {
	if !service.IsLeader() {
		resp := &mspb.AddColumnResponse{Header: &mspb.ResponseHeader{}}
		point := service.GetLeader()
		if point == nil {
			resp.Header.Error = &mspb.Error{
				NoLeader: &mspb.NoLeader{},
			}
		} else {
			resp.Header.Error = &mspb.Error{
				MsLeader: &mspb.MsLeader{MsLeader: point.RpcAddress},
			}
		}
		return resp, nil
	}
	return service.handleAddColumns(ctx, req)
}
