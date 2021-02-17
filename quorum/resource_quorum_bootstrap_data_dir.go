package quorum

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"

	"github.com/ethereum/go-ethereum/params"
	"github.com/peterbourgon/mergemap"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/node"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

// Use this resource to create a data dir locally. This equivalent to execute `geth init`.
func resourceBootstrapDataDir() *schema.Resource {
	return &schema.Resource{
		Create: resourceBootstrapDataDirCreate,
		Read:   resourceBootstrapDataDirRead,
		Delete: resourceBootstrapDataDirDelete,

		Schema: map[string]*schema.Schema{
			"data_dir": {
				Type:        schema.TypeString,
				Description: "Directory to intialize a genesis block",
				Required:    true,
				ForceNew:    true,
			},
			"instance_name": {
				Type:        schema.TypeString,
				Description: "The instance name of the node. This must be the same as the value in geth node config. Default is `geth`",
				Optional:    true,
				ForceNew:    true,
				Default:     "geth",
			},
			"genesis": {
				Type:        schema.TypeString,
				Description: "Genesis file content in JSON format",
				Required:    true,
				ForceNew:    true,
				ValidateFunc: func(i interface{}, s string) (ws []string, es []error) {
					jsonStr := i.(string)
					var g *core.Genesis
					if err := json.Unmarshal([]byte(jsonStr), &g); err != nil {
						es = append(es, err)
						return
					}
					return
				},
			},
			"data_dir_abs": {
				Type:        schema.TypeString,
				Description: "Absolute path to the data dir",
				Computed:    true,
			},
		},
	}
}

func resourceBootstrapDataDirCreate(d *schema.ResourceData, rawConfigurer interface{}) error {
	config := rawConfigurer.(*configurer)
	config.bootstrapDataDirMux.Lock()
	defer config.bootstrapDataDirMux.Unlock()
	targetDir := d.Get("data_dir").(string)
	absDir, err := createDirectory(targetDir)
	if err != nil {
		return err
	}
	nodeConfig := &node.DefaultConfig
	nodeConfig.DataDir = absDir
	nodeConfig.Name = d.Get("instance_name").(string)
	// check if the target dir is empty
	if files, err := ioutil.ReadDir(path.Join(absDir, nodeConfig.Name)); err != nil && !os.IsNotExist(err) {
		return err
	} else {
		if len(files) > 0 {
			return fmt.Errorf("directory [%s] is not empty", absDir)
		}
	}
	genesisJson := []byte(d.Get("genesis").(string))
	var genesis *core.Genesis
	var miniGenesis *struct {
		Config map[string]interface{} `json:"config"`
	}
	if err := json.Unmarshal(genesisJson, &genesis); err != nil {
		return err
	}
	log.Printf("[DEBUG] Reading ChainConfig as raw data")
	if err := json.Unmarshal(genesisJson, &miniGenesis); err != nil {
		return err
	}
	// init datadir
	stack, err := node.New(nodeConfig)
	if err != nil {
		return err
	}
	for _, name := range []string{"chaindata", "lightchaindata"} {
		chaindb, err := stack.OpenDatabase(name, 0, 0)
		if err != nil {
			return fmt.Errorf("can't open database for %s due to %s", name, err)
		}
		savedChainConfig, blockHash, err := core.SetupGenesisBlock(chaindb, genesis)
		if err != nil {
			return fmt.Errorf("can't setup genesis for %s due to %s", name, err)
		}
		// let's merge the ChainConfig and save into the database
		mergedChainConfig, err := merge(savedChainConfig, miniGenesis.Config)
		if err != nil {
			return fmt.Errorf("can't merge ChainConfig due to %v", err)
		}
		// this is a workaround as Ethereum doesn't export the config key
		if err := chaindb.Put(append([]byte("ethereum-config-"), blockHash.Bytes()...), mergedChainConfig); err != nil {
			return err
		}
		log.Printf("[DEBUG] Successfully wrote genesis state: database=%s, dir=%s", name, absDir)
	}
	_ = d.Set("data_dir_abs", absDir)
	d.SetId(fmt.Sprintf("%d", time.Now().UnixNano()))
	return nil
}

func merge(config *params.ChainConfig, dst map[string]interface{}) ([]byte, error) {
	// convert to map[string]interface{}
	configData, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	var src map[string]interface{}
	if err := json.Unmarshal(configData, &src); err != nil {
		return nil, err
	}
	// now merge
	mergedConfig := mergemap.Merge(dst, src)
	return json.Marshal(mergedConfig)
}

func resourceBootstrapDataDirRead(_ *schema.ResourceData, _ interface{}) error {
	return nil
}

func resourceBootstrapDataDirDelete(d *schema.ResourceData, _ interface{}) error {
	d.SetId("")
	dir := d.Get("data_dir_abs").(string)
	return os.RemoveAll(dir)
}
