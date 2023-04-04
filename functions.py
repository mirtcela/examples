def currency_converter(self):
        def convert_value(
            currency_from: str, currency_to: str, amount=1, rate=False, reverse=False
        ) -> float:
            """
            Function convert from one currency to another
            :param currency_from: a currency from convert
            :param currency_to: a main currency to convert
            :param amount: a value to convert
            :param rate: a flag for return currency_rate
            :param reverse: a flag for reverse pair of currency
            :return: amount in main currency
            """
            if currency_from == currency_to:
                return amount
            if reverse:
                currency_rate = self.currency_cache[(currency_to, currency_from)]
                print(
                    f"Found {currency_from} to {currency_to} currency rate {currency_rate} in cache "
                )
                if rate:
                    if currency_from == currency_to:
                        return 1
                    else:
                        return currency_rate
                else:
                    return round(float(amount * currency_rate), 4)
            else:
                if (currency_from, currency_to) not in self.currency_cache:
                    conv_json = convert(currency_to, currency_from, 1)
                    currency_rate = round(float(json.loads(conv_json)["amount"]), 4)
                    self.currency_cache[(currency_from, currency_to)] = currency_rate
                    print(
                        f"From Google get currency rate {currency_rate} from {currency_to} to {currency_from}"
                    )
                return round(
                    float(amount / self.currency_cache[(currency_from, currency_to)]), 4
                )

        return convert_value
    
def map_metric_values_to_dict(self, asset, metric_dict):
        """
        Generate dict for all preprocces metrics
        Returns
        -------
        dict
            dict with pairs ('metric's name': str, value: Decimal or None)
        """
        for metric in self.metrics_list:
            metrics_ids = self.session.scalars(
                select(Metric.id)
                .select_from(
                    join(
                        Metric,
                        MetricLabel,
                        Metric.metric_label_id == MetricLabel.id,
                        isouter=True,
                    )
                )
                .where(MetricLabel.label == metric)
            ).all()
            metric_value = (
                self.session.scalars(
                    select(MetricLookbackValue.value).where(
                        and_(
                            MetricLookbackValue.asset_id == asset,
                            MetricLookbackValue.metric_id.in_(metrics_ids),
                        )
                    )
                ).first()
                or 0
            )
            metric_dict[metric] = metric_value
      
      
async def get_assets_details(
        self,
        db,
        *,
        skip: int = 0,
        limit: int = 10,
        platform_id: int,
        for_level_no: str,
        for_ids: List[str],
        desired_level: str,
        search_key: str,
    ) -> Any:
        count = 0
        result_dict = Assets().dict()
        if desired_level == LevelNo.L0_AD.value or LevelNo.L0_KW.value:
            if for_level_no == LevelNo.L1.value:
                sel_l1_ids_for_ads = (
                    select(Asset.id)
                    .where(
                        Asset.level_no == LevelNo.L1.value,
                        Asset.id.cast(String(36)).in_(for_ids),
                    )
                    .distinct()
                )
                l1_ids_for_ads = await db.execute(sel_l1_ids_for_ads)
                l1_ids_for_ads = l1_ids_for_ads.scalars().all()
                stmt = (
                    select(Asset)
                    .where(
                        Asset.parent_id.cast(String(36)).in_(for_ids),
                        Asset.name.ilike("%" + search_key + "%"),
                        Asset.level_no == desired_level,
                    )
                    .offset(skip)
                    .limit(limit)
                )
                result = await db.execute(stmt)
                result = result.scalars().all()
                if result:
                    for item in result:
                        res_dict = {
                            "id": item.id,
                            "real_id": item.real_id,
                            "name": item.name,
                            "level_no": item.level_no,
                        }
                        if item.parent_id in l1_ids_for_ads:
                            sel_l1_item = (
                                select(Asset)
                                .where(
                                    Asset.id == item.parent_id,
                                    )
                                .distinct()
                            )
                            l1_item = await db.execute(sel_l1_item)
                            l1_item = l1_item.scalars().one()
                            res_dict["L1_id"] = item.parent_id
                            res_dict["L1_real_id"] = l1_item.real_id
                            res_dict["L1_name"] = l1_item.name
                            sel_l2_item = (
                                select(Asset)
                                .where(
                                    Asset.id == l1_item.parent_id,
                                    )
                                .distinct()
                            )
                            l2_item = await db.execute(sel_l2_item)
                            l2_item = l2_item.scalars().one()
                            res_dict["L2_id"] = l2_item.id
                            res_dict["L2_real_id"] = l2_item.real_id
                            res_dict["L2_name"] = l2_item.name
                        result_dict[f"{desired_level}_assets"].append(res_dict)  # type: ignore
                count += len(result)
            if for_level_no == LevelNo.L2.value:
                sel_l2_ids_for_ads = (
                    select(Asset.id)
                    .where(
                        Asset.level_no == LevelNo.L2.value,
                        Asset.id.cast(String(36)).in_(for_ids),
                    )
                    .distinct()
                )
                l2_ids_for_ads = await db.execute(sel_l2_ids_for_ads)
                l2_ids_for_ads = l2_ids_for_ads.scalars().all()
                for l2_id in l2_ids_for_ads:
                    sel_l1_ids_for_ads = (
                        select(Asset.id)
                        .where(
                            Asset.level_no == LevelNo.L1.value,
                            Asset.parent_id == l2_id,
                        )
                        .distinct()
                    )
                    l1_ids_for_ads = await db.execute(sel_l1_ids_for_ads)
                    l1_ids_for_ads = l1_ids_for_ads.scalars().all()
                    stmt = (
                        select(Asset)
                        .where(
                            Asset.parent_id.in_(l1_ids_for_ads),
                            Asset.name.ilike("%" + search_key + "%"),
                            Asset.level_no == desired_level,
                        )
                        .offset(skip)
                        .limit(limit)
                    )
                    result = await db.execute(stmt)
                    result = result.scalars().all()
                    if result:
                        for item in result:
                            res_dict = {
                                "id": item.id,
                                "real_id": item.real_id,
                                "name": item.name,
                                "level_no": item.level_no,
                            }
                            sel_l1_item = (
                                select(Asset)
                                .where(
                                    Asset.id == item.parent_id,
                                    Asset.parent_id == l2_id,
                                    Asset.level_no == LevelNo.L1.value,
                                )
                                .distinct()
                            )
                            l1_item = await db.execute(sel_l1_item)
                            l1_item = l1_item.scalars().one()
                            res_dict["L1_id"] = l1_item.id
                            res_dict["L1_real_id"] = l1_item.real_id
                            res_dict["L1_name"] = l1_item.name
                            sel_l2_item = (
                                select(Asset)
                                .where(
                                    Asset.id == l2_id,
                                    )
                                .distinct()
                            )
                            l2_item = await db.execute(sel_l2_item)
                            l2_item = l2_item.scalars().one()
                            res_dict["L2_id"] = l2_item.id
                            res_dict["L2_real_id"] = l2_item.real_id
                            res_dict["L2_name"] = l2_item.name
                            result_dict[f"{desired_level}_assets"].append(res_dict)
                    count += len(result)
            result_dict["total"] = count