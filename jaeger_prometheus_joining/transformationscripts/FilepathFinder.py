from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class FilepathFinder:
    def __init__(self, settings: ParseSettings):
        self.settings = settings

    def find_files(self):
        path_list = {}

        for service in self.settings.source.iterdir():
            if not service.is_dir():
                continue

            path_list[service.name] = {"monitoring": [], "traces": []}

            for category in service.iterdir():
                if not category.is_dir():
                    continue

                folder_name = category.name.lower()
                files = [x for x in category.glob("*.json") if x.stat().st_size > 100]

                if folder_name.startswith("monitor"):
                    path_list[service.name]["monitoring"].extend(files)

                if folder_name.startswith("ts") or folder_name.startswith("trace"):
                    path_list[service.name]["traces"].extend(files)

            if (
                len(path_list[service.name]["monitoring"]) == 0
                or len(path_list[service.name]["traces"]) == 0
            ):
                del path_list[service.name]

        if self.settings.test_mode:
            temp_list = {}
            temp_list['ts-admin-basic-info-service-sprintstarterweb_1.5.22'] = path_list['ts-admin-basic-info-service-sprintstarterweb_1.5.22']
            path_list = temp_list

            while len(path_list.keys()) > 2:
                path_list.popitem()

        return path_list
