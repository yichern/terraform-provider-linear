package provider

import (
	"context"
	"fmt"
	"sync"

	"github.com/Khan/genqlient/graphql"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

type ProviderData struct {
	Client graphql.Client
	Cache  BulkCache
}

// BulkCache provides lazy-loaded caches for list queries, reducing
// per-resource API calls to one list query per resource type.
type BulkCache struct {
	client graphql.Client

	labelsOnce sync.Once
	labels     map[string]*IssueLabel
	labelsErr  error

	workflowStatesOnce sync.Once
	workflowStates     map[string]*WorkflowState
	workflowStatesErr  error

	templatesOnce sync.Once
	templates     map[string]*Template
	templatesErr  error

	teamsOnce  sync.Once
	teamsByKey map[string]*Team
	teamsErr   error
}

func newBulkCache(client graphql.Client) BulkCache {
	return BulkCache{client: client}
}

func (c *BulkCache) ensureLabels(ctx context.Context) {
	c.labelsOnce.Do(func() {
		tflog.Debug(ctx, "bulk fetching all issue labels")
		c.labels = make(map[string]*IssueLabel)
		var cursor *string
		for {
			resp, err := listAllLabelsPage(ctx, c.client, cursor)
			if err != nil {
				c.labelsErr = err
				return
			}
			for _, node := range resp.IssueLabels.Nodes {
				label := node.IssueLabel
				c.labels[label.Id] = &label
			}
			if !resp.IssueLabels.PageInfo.HasNextPage {
				break
			}
			cursor = &resp.IssueLabels.PageInfo.EndCursor
		}
		tflog.Debug(ctx, fmt.Sprintf("bulk fetched %d issue labels", len(c.labels)))
	})
}

func (c *BulkCache) GetLabel(ctx context.Context, id string) (*IssueLabel, error) {
	c.ensureLabels(ctx)
	if c.labelsErr != nil {
		return nil, c.labelsErr
	}
	label, ok := c.labels[id]
	if !ok {
		return nil, fmt.Errorf("label not found in bulk cache: %s", id)
	}
	return label, nil
}

func (c *BulkCache) ensureWorkflowStates(ctx context.Context) {
	c.workflowStatesOnce.Do(func() {
		tflog.Debug(ctx, "bulk fetching all workflow states")
		c.workflowStates = make(map[string]*WorkflowState)
		var cursor *string
		for {
			resp, err := listAllWorkflowStatesPage(ctx, c.client, cursor)
			if err != nil {
				c.workflowStatesErr = err
				return
			}
			for _, node := range resp.WorkflowStates.Nodes {
				ws := node.WorkflowState
				c.workflowStates[ws.Id] = &ws
			}
			if !resp.WorkflowStates.PageInfo.HasNextPage {
				break
			}
			cursor = &resp.WorkflowStates.PageInfo.EndCursor
		}
		tflog.Debug(ctx, fmt.Sprintf("bulk fetched %d workflow states", len(c.workflowStates)))
	})
}

func (c *BulkCache) GetWorkflowState(ctx context.Context, id string) (*WorkflowState, error) {
	c.ensureWorkflowStates(ctx)
	if c.workflowStatesErr != nil {
		return nil, c.workflowStatesErr
	}
	ws, ok := c.workflowStates[id]
	if !ok {
		return nil, fmt.Errorf("workflow state not found in bulk cache: %s", id)
	}
	return ws, nil
}

func (c *BulkCache) GetWorkflowStatesByTeamID(ctx context.Context, teamID string) ([]WorkflowState, error) {
	c.ensureWorkflowStates(ctx)
	if c.workflowStatesErr != nil {
		return nil, c.workflowStatesErr
	}
	var result []WorkflowState
	for _, ws := range c.workflowStates {
		if ws.Team.Id == teamID {
			result = append(result, *ws)
		}
	}
	return result, nil
}

func (c *BulkCache) ensureTemplates(ctx context.Context) {
	c.templatesOnce.Do(func() {
		tflog.Debug(ctx, "bulk fetching all templates")
		resp, err := listAllTemplates(ctx, c.client)
		if err != nil {
			c.templatesErr = err
			return
		}
		c.templates = make(map[string]*Template, len(resp.Templates))
		for _, node := range resp.Templates {
			t := node.Template
			c.templates[t.Id] = &t
		}
		tflog.Debug(ctx, fmt.Sprintf("bulk fetched %d templates", len(c.templates)))
	})
}

func (c *BulkCache) GetTemplate(ctx context.Context, id string) (*Template, error) {
	c.ensureTemplates(ctx)
	if c.templatesErr != nil {
		return nil, c.templatesErr
	}
	t, ok := c.templates[id]
	if !ok {
		return nil, fmt.Errorf("template not found in bulk cache: %s", id)
	}
	return t, nil
}

func (c *BulkCache) ensureTeams(ctx context.Context) {
	c.teamsOnce.Do(func() {
		tflog.Debug(ctx, "bulk fetching all teams")
		c.teamsByKey = make(map[string]*Team)
		var cursor *string
		for {
			resp, err := listAllTeamsPage(ctx, c.client, cursor)
			if err != nil {
				c.teamsErr = err
				return
			}
			for _, node := range resp.Teams.Nodes {
				team := node.Team
				c.teamsByKey[team.Key] = &team
			}
			if !resp.Teams.PageInfo.HasNextPage {
				break
			}
			cursor = &resp.Teams.PageInfo.EndCursor
		}
		tflog.Debug(ctx, fmt.Sprintf("bulk fetched %d teams", len(c.teamsByKey)))
	})
}

func (c *BulkCache) GetTeamByKey(ctx context.Context, key string) (*Team, error) {
	c.ensureTeams(ctx)
	if c.teamsErr != nil {
		return nil, c.teamsErr
	}
	team, ok := c.teamsByKey[key]
	if !ok {
		return nil, fmt.Errorf("team not found in bulk cache: %s", key)
	}
	return team, nil
}
