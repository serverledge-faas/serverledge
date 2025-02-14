package asl

import (
	"fmt"

	"github.com/buger/jsonparser"
	"github.com/labstack/gommon/log"
	"github.com/serverledge-faas/serverledge/internal/types"
)

type ChoiceState struct {
	Type       StateType    // Necessary
	Choices    []ChoiceRule // Necessary. All DataTestExpression must be State Machine with an end, like workflow.ChoiceNode(s).
	InputPath  Path         // Optional
	OutputPath Path         // Optional
	// Default is the default state to execute when no other DataTestExpression matches
	Default string // Optional, but to avoid errors it is highly recommended.
}

func (c *ChoiceState) ParseFrom(jsonData []byte) (State, error) {
	c.Type = StateType(JsonExtractStringOrDefault(jsonData, "Type", "Choice"))
	c.InputPath = JsonExtractRefPathOrDefault(jsonData, "InputPath", "")
	c.OutputPath = JsonExtractRefPathOrDefault(jsonData, "OutputPath", "")
	c.Default = JsonExtractStringOrDefault(jsonData, "Default", "")

	choiceRules := make([]ChoiceRule, 0)

	choices, errChoice := JsonExtract(jsonData, "Choices")
	if errChoice != nil {
		return nil, fmt.Errorf("failed to parse Choices %v", errChoice)
	}

	_, _ = jsonparser.ArrayEach(choices, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		cr, errR := ParseRule(value)
		if errR != nil {
			log.Errorf("failed to parse choice rule %d: %v", offset, err)
			return
		}
		choiceRules = append(choiceRules, cr)
	})
	//if errArr != nil {
	//	return nil, fmt.Errorf("error %v when parsing choice rule %s", errArr, choices[:offset])
	//}
	c.Choices = choiceRules

	return c, nil
}

func (c *ChoiceState) Validate(stateNames []string) error {
	if c.Default == "" {
		log.Warn("Default choice not specified")
	}
	return nil
}

func NewEmptyChoice() *ChoiceState {
	return &ChoiceState{
		Type:       Choice,
		Choices:    []ChoiceRule{},
		InputPath:  "",
		OutputPath: "",
		Default:    "",
	}
}

func (c *ChoiceState) GetType() StateType {
	return Choice
}

// GetNext for ChoiceState returns the Default branch instead of next
func (c *ChoiceState) GetNext() (string, bool) {
	if c.Default != "" {
		return c.Default, true
	}
	return "", false
}

// IsEndState always returns true for a ChoiceState, because it is always a terminal state.
func (c *ChoiceState) IsEndState() bool {
	return true
}

func (c *ChoiceState) Equals(cmp types.Comparable) bool {
	c2 := cmp.(*ChoiceState)

	if len(c.Choices) != len(c2.Choices) {
		return false
	}

	for i, c1 := range c.Choices {
		if !c1.Equals(c2.Choices[i]) {
			return false
		}
	}

	return c.Type == c2.Type &&
		c.InputPath == c2.InputPath &&
		c.OutputPath == c2.OutputPath &&
		c.Default == c2.Default
}

func (c *ChoiceState) String() string {
	str := fmt.Sprint("{",
		"\n\t\t\tType: ", c.Type,
		"\n\t\t\tDefault: ", c.Default,
		"\n\t\t\tChoices: [")
	for i, c1 := range c.Choices {
		str += c1.String()
		if i < len(c.Choices)-1 {
			str += ","
		}
	}
	str += "\n\t\t\t]\n"

	if c.InputPath != "" {
		str += fmt.Sprintf("\t\t\tInputPath: %s\n", c.InputPath)
	}
	if c.OutputPath != "" {
		str += fmt.Sprintf("\t\t\tOutputPath: %s\n", c.OutputPath)
	}
	str += "\t\t}"
	return str
}
