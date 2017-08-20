// +build 386 windows,amd64 windows
// Copied from https://github.com/streamsets/windataextractor/tree/master/dev/src/lib/win/eventlog

package windows

import (
	"fmt"
	"golang.org/x/sys/windows/registry"
	"golang.org/x/sys/windows/svc/mgr"
	"log"
	"syscall"
	"unsafe"
)

type ServiceStore struct {
	serviceName string
	prefix      string
}

func NewServiceConfig(serviceName string) *ServiceStore {
	return &ServiceStore{serviceName, "config."}
}

func NewServiceData(serviceName string) *ServiceStore {
	return &ServiceStore{serviceName, "data."}
}

func NewServiceRaw(serviceName string) *ServiceStore {
	return &ServiceStore{serviceName, ""}
}

func (serviceStore *ServiceStore) Write(config map[string]string) error {
	return writeToServiceKey(serviceStore.serviceName, serviceStore.prefix, config)
}

func (serviceStore *ServiceStore) Read(configKeys []string) (map[string]string, error) {
	return readFromServiceKey(serviceStore.serviceName, serviceStore.prefix, configKeys)
}

func doIfServiceExists(serviceName string, action func() error) error {
	if m, err := mgr.Connect(); err == nil {
		defer m.Disconnect()
		if s, err := m.OpenService(serviceName); err == nil {
			defer s.Close()
			return action()
		} else {
			return fmt.Errorf("service %s is not installed", serviceName)
		}
	} else {
		return err
	}
}

func getServiceKey(serviceName string) string {
	return `SYSTEM\CurrentControlSet\Services\` + serviceName
}

func doWithServiceKey(serviceName string, action func(key *registry.Key) error) error {
	if sKey, err := registry.OpenKey(syscall.HKEY_LOCAL_MACHINE, getServiceKey(serviceName), 0); err == nil {
		defer sKey.Close()
		return action(&sKey)
	} else {
		return err
	}
}

func writeToServiceKey(serviceName string, prefix string, data map[string]string) error {
	prefixedData := make(map[string]string)
	for k, v := range data {
		prefixedData[prefix+k] = v
	}
	return writeToRegistryKey(getServiceKey(serviceName), prefixedData)
}

func readFromServiceKey(serviceName string, prefix string, dataKeys []string) (map[string]string, error) {
	prefixedDataKeys := make([]string, 0)
	for _, k := range dataKeys {
		prefixedDataKeys = append(prefixedDataKeys, prefix+k)
	}
	if data, err := ReadFromRegistryKey(getServiceKey(serviceName), prefixedDataKeys); err == nil {
		unprefixedData := make(map[string]string)
		for k, v := range data {
			unprefixedData[k[len(prefix):]] = v
		}
		return unprefixedData, nil
	} else {
		return nil, err
	}
}

func doWithKey(keyPath string, action func(key *registry.Key) error) error {
	if sKey, err := registry.OpenKey(syscall.HKEY_LOCAL_MACHINE, keyPath, 0); err == nil {
		defer sKey.Close()
		log.Printf("[DEBUG] Registry - Opened key=%s", keyPath)
		return action(&sKey)
	} else {
		log.Printf("[DEBUG] Registry - Could not open key=%s, error %v", keyPath, err)
		return err
	}
}

func ReadFromRegistryKey(keyPath string, dataKeys []string) (map[string]string, error) {
	data := make(map[string]string)
	readFromKey := func(key *registry.Key) error {
		for _, dataK := range dataKeys {
			var typ uint32
			var buffer [syscall.MAX_LONG_PATH]uint16
			n := uint32(len(buffer))
			registryKey := *key
			hand := syscall.Handle(registryKey)
			err := syscall.RegQueryValueEx(hand, syscall.StringToUTF16Ptr(dataK), nil,
				&typ, (*byte)(unsafe.Pointer(&buffer[0])), &n)
			if err != nil && err != syscall.ERROR_FILE_NOT_FOUND {
				log.Printf("[ERROR] err %d %v", n, err)
				return err
			} else {
				if err == nil {
					data[dataK] = syscall.UTF16ToString(buffer[:])
				}
			}
		}
		return nil
	}
	log.Printf("[DEBUG] Registry - Reading key=%s values=%v", keyPath, dataKeys)
	if err := doWithKey(keyPath, readFromKey); err == nil {
		log.Printf("[DEBUG] Registry - Read key=%s values=%v", keyPath, data)
		return data, nil
	} else {
		return nil, err
	}
}

func writeToRegistryKey(keyPath string, data map[string]string) error {
	writeInKey := func(key *registry.Key) error {
		for dataK, dataV := range data {
			if err := key.SetStringValue(dataK, dataV); err != nil {
				return err
			}
		}
		return nil
	}
	log.Printf("[DEBUG] Registry - Writing key=%s values=%v", keyPath, data)
	if err := doWithKey(keyPath, writeInKey); err != nil {
		return err
	} else {
		log.Printf("[DEBUG] ServiceStore - Written key=%s values=%v", keyPath, data)
		return nil
	}
}
