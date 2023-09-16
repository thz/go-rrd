package rrd

import (
	"fmt"
	"strconv"
	"strings"
)

// Info represents the configuration information of an RRD.
type Info struct {
	Key   string
	Value interface{}
}

func (c *Client) InfoMap(filename string) (map[string]interface{}, error) {
	infoList, err := c.Info(filename)
	if err != nil {
		return nil, err
	}
	infoMap := map[string]interface{}{}
	for idx := range infoList {
		infoMap[infoList[idx].Key] = infoList[idx].Value
	}
	return infoMap, nil
}

// Info returns the configuration information for the specified RRD.
func (c *Client) Info(filename string) ([]*Info, error) {
	lines, err := c.ExecCmd(NewCmd("info").WithArgs(filename))
	if err != nil {
		return nil, fmt.Errorf("failed to get info for '%s': %w", filename, err)
	}

	data := make([]*Info, len(lines))
	for i, line := range lines {
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("unexpected response: not 3 parts '%s'", line)
		}
		info := &Info{Key: parts[0]}
		switch parts[1] {
		case "2":
			// string
			info.Value = parts[2]
		case "1":
			// int
			v, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				return nil, NewInvalidResponseError(fmt.Sprintf("info: invalid int for key %v", info.Key), line)
			}
			info.Value = v
		case "0":
			// float
			v, err := strconv.ParseFloat(parts[2], 64)
			if err != nil {
				return nil, NewInvalidResponseError(fmt.Sprintf("info: invalid float for key %v", info.Key), line)
			}
			info.Value = v
		default:
			return nil, NewInvalidResponseError(fmt.Sprintf("info: unknown type %v for key %v", parts[1], info.Key), line)
		}
		data[i] = info
	}

	return data, nil
}
