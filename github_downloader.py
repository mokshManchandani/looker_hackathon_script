# this module takes care of fetching the github repository to our code base
from pathlib import Path
import git
class GitDownloader:
    # this is just by preference can be changed
    # all the repos that are cloned will be placed in the data directory
    root_path = Path("data")
    def __init__(self, repo_link, local_dir, github_config=None):
        self.repo_link = repo_link
        self.local_dir = GitDownloader.root_path / Path(local_dir)
        self.view_file_glob=None
        self.model_file_glob=None

        #NOTE: This is important while fetching private repos
        # for public repos we can leave out the github_config and personal access tokens
        self.authenticated_url = None
        if github_config and 'pat' in github_config:
            self.authenticated_url = f"https://oauth2:{github_config['pat']}@{self.repo_link.split('//')[1]}"



    def __get_files_glob(self):
        """
        Fetch the list of view_files and model_file(s)
        """
        self.view_file_glob = [*self._views.glob("*.lkml")]
        self.model_file_glob = [*self._models.glob('*.model.lkml')]
        


    def download_content(self):
        """
        this function takes care of a clone and a pull
        """
        # pull scenario
        if self.local_dir.is_dir():
            _repo = git.Repo(str(self.local_dir))
            _origin = _repo.remotes.origin
            _origin.pull()
        else:
            # clone
            _repo_link = self.authenticated_url if self.authenticated_url else self.repo_link
            git.Repo.clone_from(_repo_link,self.local_dir)
        
        self._views = self.local_dir / "views"
        self._models = self.local_dir / "models"

        # for my use case it was kind of a compulsion to have a model file inside the models folder
        if not self._views.is_dir() and not self._models.is_dir():
            print("Download failed retry please make sure views are present in view folder and models are present in models folder")
            return
        self.__get_files_glob()
      

    def __repr__(self):
        return f"GitDownloader({self.repo_link=},{self.local_dir=})"