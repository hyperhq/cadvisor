package hyper

import (
	"fmt"
	"regexp"
	"strings"
)

func isHyperVirtualMachine(name string) (string, error) {
	re := regexp.MustCompile("vm-[0-9a-zA-Z]+")
	vmName := re.FindString(strings.Replace(name, "\\x2d", "-", -1))
	if len(vmName) > 0 {
		return vmName, nil
	}

	return "", fmt.Errorf("%s isn't a hyper vm", name)
}
